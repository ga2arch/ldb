(ns ldb.db
  (:require [clojure.data.fressian :as fress])
  (:import (org.lmdbjava Env DbiFlags PutFlags KeyRange CursorIterator Dbi)
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

(defn scan-all
  [db txn]
  (let [^CursorIterator it (.iterate db txn (KeyRange/all))]
    (let [vals (volatile! {})]
      (while (.hasNext it)
        (let [kv (.next it)]
          (vswap! vals (fn [vals] (update vals (decode (.key kv))
                                          (fn [a] ((fnil conj []) a (decode (.val kv)))))))))
      (into {} @vals))))

(defn get-key
  [^Dbi db txn key]
  (when-let [v (.get db txn (encode-key key))]
    (decode v)))

(defn scan-key
  [db txn key]
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
            (.close it)))))))

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

;;

(defn entity-by-id
  [conn eid]
  (with-open [txn (txn-read conn)]
    (let [xf (map (fn [[_ attr value _ _]] [attr value]))]
      (into {:db/id eid} xf (scan-key (:eavt-current conn) txn eid)))))

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
              (get-key (:status conn) txn :last-tid))]
    (or tid 0)))

(defn find-datom
  [conn txn [eid attr value]]
  (let [xf (comp
             (filter (fn [[ceid cattr cvalue]] (and (= ceid eid) (= cattr attr) (= cvalue value))))
             (halt-when any?))]
    (transduce xf identity nil (scan-key (:eavt-current conn) txn eid))))

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
    {:eavt {:current (scan-all eavt-current txn)
            :history (scan-all eavt-history txn)}
     :aevt {:current (scan-all aevt-current txn)
            :history (scan-all aevt-history txn)}}))

;; query

(defn is-binding-var
  [x]
  (when x
    (.startsWith (name x) "?")))

(defn load-datoms
  [conn txn [eid attr val]]
  (let [filter-attr (filter (fn [[_ dattr _]] (= dattr attr)))
        filter-val (filter (fn [[_ _ dval]] (= dval val)))]
    (cond
      (not (is-binding-var eid))
      (cond
        (and (not (is-binding-var attr))
             (not (is-binding-var val)))
        (eduction filter-attr
                  filter-val
                  (scan-key (:eavt-current conn) txn eid))

        (not (is-binding-var attr))
        (eduction filter-attr
                  (scan-key (:eavt-current conn) txn eid))

        (not (is-binding-var val))
        (eduction filter-val
                  (scan-key (:eavt-current conn) txn eid))

        :else
        (scan-key (:eavt-current conn) txn eid))

      (not (is-binding-var attr))
      (if-not (is-binding-var val)
        (eduction filter-attr
                  (scan-key (:aevt-current conn) txn attr))
        (scan-key (:aevt-current conn) txn attr))

      :else
      (if-not (is-binding-var val)
        (eduction
          (mapcat second)
          filter-val
          (scan-all (:aevt-current conn) txn))

        (eduction
          (mapcat second)
          (scan-all (:aevt-current conn) txn))))))

(defn match-datom
  [frame pattern datom]
  (letfn [(reducing-fn [frame [i binding-var]]
            (let [binded-var (get frame binding-var binding-var)
                  datom-var (nth datom i)]
              (if (is-binding-var binding-var)
                (assoc frame binding-var datom-var)

                (if (= datom-var binded-var)
                  frame
                  (reduced nil)))))]
    (reduce reducing-fn frame (map-indexed vector pattern))))

(defn match-pattern
  [frame pattern]
  (comp
    (map (partial match-datom frame pattern))
    (filter (comp not nil?))))

(defn unify
  [frame pattern]
  (mapv #(get frame % %) pattern))

(defn entities-by-id [conn eids]
  (map (fn [[eid]] (entity-by-id conn eid)) eids))

(defn q
  [conn {:keys [find where]}]
  (letfn [(reducing-fn
            [txn frames pattern]
            (mapcat (fn [frame]
                      (let [datoms (load-datoms conn txn (unify frame pattern))]
                        (into [] (match-pattern frame pattern) datoms))) frames))]

    (let [frames (with-open [txn (txn-read conn)]
                   (reduce (partial reducing-fn txn) [{}] where))]
      (for [frame frames]
        (for [f find]
          (get frame f))))))
