(ns db
  (:require [clojure.data.fressian :as fress])
  (:import (java.nio.file Files Path)
           (org.lmdbjava Env DbiFlags Dbi PutFlags KeyRange Txn CursorIterator)
           (java.io File)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.util Iterator UUID)
           (clojure.lang IReduceInit)))

(defprotocol ADatom
  (to-eavt [this])
  (to-aevt [this]))

(defrecord Datom [eid attr value tid op]
  ADatom
  (to-eavt [this] [eid attr value tid op])
  (to-aevt [this] [attr eid value tid op]))

(def bkey (ByteBuffer/allocateDirect 511))
(def bval (ByteBuffer/allocateDirect 2000))

(defn encode-key
  [data]
  (.clear bkey)

  (cond
    (keyword? data)
    (.put bkey (.getBytes (name data) StandardCharsets/UTF_8))

    (string? data)
    (.put bkey (.getBytes data StandardCharsets/UTF_8))

    (int? data)
    (.putInt bkey data))

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
          (vswap! vals (fn [vals] (update vals (str (.decode StandardCharsets/UTF_8 (.key kv)))
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


(defn open-connection
  [{:db/keys [filepath]}]
  (let [env (-> (Env/create)
                (.setMapSize 10485760)
                (.setMaxDbs 6)
                (.open filepath nil))]
    (letfn [(open-db [name]
              (.openDbi env name (into-array DbiFlags [DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT])))]
      {:env          env
       :eavt-current (open-db "eavt-current")
       :eavt-history (open-db "eavt-history")
       :aevt-current (open-db "aevt-current")
       :aevt-history (open-db "aevt-history")
       :status       (.openDbi env "status" (into-array DbiFlags [DbiFlags/MDB_CREATE]))})))

(def db {:db/filepath (File. ".")})
(def conn (open-connection db))

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
      (into {} xf (get-key (:eavt-current conn) txn eid)))))

(defn data->actions
  [conn data]
  (cond
    (map? data)
    (let [eid (or (:db/id data) (str (UUID/randomUUID)))
          entity (entity-by-id conn eid)]
      (for [[attr value] data
            :let [old-value (get entity attr)]
            :when (and (not= attr :db/id)
                       (not= value old-value))]
        (if old-value
          [[:db/retract eid attr old-value]
           [:db/add eid attr value]]

          [[:db/add eid attr value]])))

    (vector? data)
    (let [[action eid :as all] data]
      (case action
        :db.fn/retractEntity
        (if-let [entity (entity-by-id conn eid)]
          (for [[attr value] entity
                :when (not= attr :db/id)]
            [:db/retract eid attr value])
          (throw (ex-info "entity not found" {:eid eid})))

        :db/add
        [[all]]

        :db/retract
        [[all]]))))

(defn get-last-tid
  [conn]
  (let [tid (with-open [txn (txn-read conn)]
              (first (into [] (get-key (:status conn) txn :last-tid))))]
    (or tid 0)))

(defn from-eavt
  [[eid attr value tid op]]
  (Datom. eid attr value tid op))

(defn find-datom
  [conn txn datom]
  (let [xf (comp
             (map from-eavt)
             (halt-when (fn [d] (and
                                  (= (.-eid d) (.-eid datom))
                                  (= (.-attr d) (.-attr datom))
                                  (= (.-value d) (.-value datom))))))]
    (transduce xf conj [] (get-key (:eavt-current conn) txn (.-eid datom)))))

(defn update-indexes
  [txn datom]
  (if (.-op datom)
    (do
      (put-key (:eavt-current conn) txn (.-eid datom) (to-eavt datom))
      (put-key (:aevt-current conn) txn (.-attr datom) (to-aevt datom)))

    (let [to-retract (find-datom conn txn datom)]
      (put-key (:eavt-history conn) txn (.-eid datom) (to-eavt datom))
      (put-key (:aevt-history conn) txn (.-attr datom) (to-aevt datom))

      (del-kv (:eavt-current conn) txn (.-eid to-retract) (to-eavt to-retract))
      (del-kv (:aevt-current conn) txn (.-attr to-retract) (to-aevt to-retract)))))

(defn transaction
  [conn data]
  (let [tx-data (:tx-data data)
        tid (get-last-tid conn)]
    (with-open [txn (txn-write conn)]
      (let [xf (comp
                 (mapcat (partial data->actions conn))
                 cat
                 (map (fn [[action eid attr value]] (Datom. eid attr value tid (= :db/add action))))
                 (map (partial update-indexes txn)))]
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