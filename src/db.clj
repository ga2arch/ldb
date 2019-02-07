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

(defrecord Datom [eid attr value tid op])

(def bkey (ByteBuffer/allocateDirect 511))
(def bval (ByteBuffer/allocateDirect 2000))

(defn encode-key
  [data]
  (doto bkey
    (.clear)
    (.put (.getBytes data StandardCharsets/UTF_8))
    (.flip)))

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
  [{db :db} txn key val]
  (.put db txn (encode-key key) (encode-val val) (into-array PutFlags [])))

(defn get-key
  ([{db :db} txn]
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
                            (recur (rf result [input (decode (.val (.next it)))]))
                            res)))]
              (ensure-reduced res))
            (finally
              (.close it))))))))

  ([{db :db} txn key]
   (reify IReduceInit
     (reduce [this f init]
       (let [ekey (encode-key key)
             ^CursorIterator it (.iterate db txn (KeyRange/closed ekey ekey))]
         (try
           (loop [state init]
             (if (reduced? state)
               @state
               (if (.hasNext it)
                 (recur (f state [key (decode (.val (.next it)))]))
                 state)))
           (finally
             (.close it))))))))

(defn del-key
  [{db :db} txn key]
  (.delete db txn (encode-key key)))

(defn del-kv
  [{db :db} txn key val]
  (.delete db txn (encode-key key) (encode-val val)))

(defn close
  [{env :env}]
  (.close env))


(defn open-connection
  [{:db/keys [name filepath]}]
  (let [env (-> (Env/create)
                (.setMapSize 10485760)
                (.setMaxDbs 5)
                (.open filepath nil))
        db (.openDbi env name (into-array DbiFlags [DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT]))]
    {:env env
     :db  db}))

(def db {:db/name "prova" :db/filepath (File. ".")})
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

