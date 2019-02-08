(ns ldb.db
  (:require [clojure.data.fressian :as fress])
  (:import (org.lmdbjava Env DbiFlags PutFlags KeyRange CursorIterator)
           (java.io File)
           (java.nio ByteBuffer)
           (java.util UUID)
           (clojure.lang IReduceInit)))

(def bkey (ByteBuffer/allocateDirect 511))
(def bval (ByteBuffer/allocateDirect 2000))

(defn encode-key
  [data]
  (.clear bkey)
  (cond
    (int? data)
    (.putInt bkey data)
    :else
    (.put bkey (fress/write data)))
  (.flip bkey))

(defn encode-val
  [data]
  (doto bval
    (.clear)
    (.put (fress/write data))
    (.flip)))

(defn decode
  [data]
  (fress/read data))

(defn txn-read
  [{env :env}]
  (.txnRead env))

(defn txn-write
  [{env :env}]
  (.txnWrite env))

(defn txn-commit
  [txn]
  (.commit txn))

(defn put-key
  [db txn key val]
  (.put db txn (encode-key key) (encode-val val) (into-array PutFlags [])))

(defn get-keys
  [db txn]
  (let [^CursorIterator it (.iterate db txn (KeyRange/all))]
    (let [vals (volatile! {})]
      (while (.hasNext it)
        (let [kv (.next it)]
          (vswap! vals (fn [vals] (update vals (decode (.key kv))
                                          (fn [a] ((fnil conj []) a (decode (.val kv)))))))))
      (into {} @vals))))

(defn get-key
  ([db txn]
   (fn [rf]
     (fn
       ([] (rf))
       ([result] (rf result))
       ([result input]
        (let [ekey (encode-key input)
              ^CursorIterator it (.iterate db txn (KeyRange/closed ekey ekey))]
          (try
            (let [res (loop [res result]
                        (if (reduced? res)
                          res
                          (if (.hasNext it)
                            (recur (rf result (decode (.val (.next it)))))
                            res)))]
              (ensure-reduced res))
            (finally
              (.close it))))))))

  ([db txn key]
   (reify IReduceInit
     (reduce [this f init]
       (let [ekey (encode-key key)
             ^CursorIterator it (.iterate db txn (KeyRange/closed ekey ekey))]
         (try
           (loop [state init]
             (if (reduced? state)
               @state
               (if (.hasNext it)
                 (recur (f state (decode (.val (.next it)))))
                 state)))
           (finally
             (.close it))))))))

(defn del-key
  [db txn key]
  (.delete db txn (encode-key key)))

(defn del-kv
  [db txn key val]
  (.delete db txn (encode-key key) (encode-val val)))

(defn close
  [{env :env}]
  (.close env))


(defn connect
  [filepath]
  (let [env (-> (Env/create)
                (.setMapSize 10485760)
                (.setMaxDbs 6)
                (.open (File. filepath) nil))]
    (letfn [(open-db [name]
              (.openDbi env name (into-array DbiFlags [DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT])))]
      {:env          env
       :eavt-current (open-db "eavt-current")
       :eavt-history (open-db "eavt-history")
       :aevt-current (open-db "aevt-current")
       :aevt-history (open-db "aevt-history")
       :status       (.openDbi env "status" (into-array DbiFlags [DbiFlags/MDB_CREATE]))})))

(defn transact
  [conn txn xform-f coll]
  (try
    (let [res (into [] (xform-f conn txn) coll)]
      (when-not (.isReadOnly txn)
        (txn-commit txn))
      res)
    (finally
      (.close txn))))

(defn tx-read
  [conn xf coll]
  (with-open [txn (txn-read conn)]
    (transact conn txn xf coll)))

;;

(defn entity-by-id
  [conn eid]
  (with-open [txn (txn-read conn)]
    (let [xf (map (fn [[_ attr value _ _]] [attr value]))]
      (into {:db/id eid} xf (get-key (:eavt-current conn) txn eid)))))

(defn data->actions
  [conn tid data]
  (cond
    (map? data)
    (let [eid (or (:db/id data) (str (UUID/randomUUID)))
          entity (entity-by-id conn eid)]

      (for [[attr value] data
            :let [old-value (get entity attr)]
            :when (and (not= attr :db/id)
                       (not= value old-value))]
        (if old-value
          [[eid attr old-value tid false]
           [eid attr value tid true]]

          [[eid attr value tid true]])))

    (vector? data)
    (let [[action eid attr value] data]
      (case action
        :db.fn/retractEntity
        (if-let [entity (entity-by-id conn eid)]
          (for [[attr value] entity
                :when (not= attr :db/id)]
            [[eid attr value tid false]])
          (throw (ex-info "entity not found" {:eid eid})))

        :db/add
        [[action eid attr value true]]

        :db/retract
        [[action eid attr value false]]))))

(defn get-last-tid
  [conn]
  (let [tid (with-open [txn (txn-read conn)]
              (first (into [] (get-key (:status conn) txn :last-tid))))]
    (or tid 0)))

(defn find-datom
  [conn txn [eid attr value]]
  (let [xf (comp
             (filter (fn [[ceid cattr cvalue]] (and (= ceid eid) (= cattr attr) (= cvalue value))))
             (halt-when any?))]
    (transduce xf identity nil (get-key (:eavt-current conn) txn eid))))

(defn update-indexes
  [conn txn [eid attr _ _ op :as datom]]
  (if op
    (do
      (put-key (:eavt-current conn) txn eid datom)
      (put-key (:aevt-current conn) txn attr datom))

    (let [[reid rattr :as to-retract] (find-datom conn txn datom)]
      (put-key (:eavt-history conn) txn eid datom)
      (put-key (:aevt-history conn) txn attr datom)

      (del-kv (:eavt-current conn) txn reid to-retract)
      (del-kv (:aevt-current conn) txn rattr to-retract))))

(defn transaction
  [conn data]
  (let [tx-data (:tx-data data)
        tid (get-last-tid conn)]
    (with-open [txn (txn-write conn)]
      (let [xf (comp
                 (mapcat (partial data->actions conn tid))
                 cat
                 (map (partial update-indexes conn txn)))]
        (into [] xf tx-data)
        (put-key (:status conn) txn :last-tid (inc tid))
        (txn-commit txn)))))

(defn show-db
  [{:keys [eavt-current eavt-history aevt-current aevt-history] :as conn}]
  (with-open [txn (txn-read conn)]
    {:eavt {:current (get-keys eavt-current txn)
            :history (get-keys eavt-history txn)}
     :aevt {:current (get-keys aevt-current txn)
            :history (get-keys aevt-history txn)}}))

;; query

(defn is-binding-var
  [x]
  (.startsWith (name x) "?"))

(defn datoms-by-eid [conn eid]
  (with-open [txn (txn-read conn)]
    (into [] (get-key (:eavt-current conn) txn eid))))

(defn datoms-by-attr [conn attr]
  (with-open [txn (txn-read conn)]
    (into [] (get-key (:aevt-current conn) txn attr))))

(defn all-datoms [conn]
  (with-open [txn (txn-read conn)]
    (into [] (mapcat second) (get-keys (:aevt-current conn) txn))))

(defn load-datoms
  [conn [eid attr _]]
  (cond
    (not (is-binding-var eid))
    (datoms-by-eid conn eid)

    (not (is-binding-var attr))
    (datoms-by-attr conn attr)

    :else
    (all-datoms conn)))

(defn match-datom
  [frame pattern datom]
  (reduce (fn [frame [i binding-var]]
            (let [binded-var (get frame binding-var binding-var)
                  datom-var (nth datom i)]
              (if (is-binding-var binding-var)
                (assoc frame binding-var datom-var)

                (if (= datom-var binded-var)
                  frame
                  (reduced nil)))))
          frame (map-indexed vector pattern)))

(defn match-pattern
  [frame pattern datoms]
  (reduce (fn [frames datom]
            (if (last datom)
              (if-let [frame (match-datom frame pattern datom)]
                (conj frames frame)
                frames)
              frames)) [] datoms))

(defn unify
  [frame pattern]
  (map (fn [var] (get frame var var)) pattern))

(defn entities-by-id [conn eids]
  (map (fn [[eid]] (entity-by-id conn eid)) eids))

(defn q
  [conn {:keys [find where]}]
  (let [frames (reduce
                 (fn [frames pattern]
                   (mapcat #(match-pattern % pattern (load-datoms conn (unify % pattern))) frames))
                 [{}] where)]
    (for [frame frames]
      (for [f find]
        (get frame f)))))
