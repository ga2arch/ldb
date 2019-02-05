(ns db
  (:import (java.nio.file Files Path)
           (org.lmdbjava Env DbiFlags Dbi)
           (java.io File)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)))

(defprotocol ADatom
  (to-eavt [this])
  (to-aevt [this]))

(defrecord Datom [eid attr value tid op])

(defprotocol ALdb
  (get-index [this name])
  (update-index [this name key datoms])
  (from-index [this name key])
  (append-log [this datoms]))

(defprotocol AConnection
  (get-key [this key])
  (put-key [this key data])
  (del-key [this key])
  (close [this]))

(defprotocol AStorage
  (open-conn [this options]))

(defprotocol ADatabase
  (conn [this]))

(defrecord Database [name file storage]
  ADatabase
  (conn [this]
    (open-conn storage this)))

(def bkey (ByteBuffer/allocateDirect 511))
(def bval (ByteBuffer/allocateDirect 2000))

(defn key->bytes [data]
  (-> bkey
      (.put (.getBytes data "UTF-8"))
      (.flip))
  bkey)

(defn val->bytes [data]
  (-> bval
      (.put (.getBytes data "UTF-8"))
      (.flip))
  bval)

(defn clear [& xs]
  (doseq [[^ByteBuffer x] xs]
    (.clear x)))

(deftype LmdbConnection [^Env env ^Dbi db]
  AConnection
  (put-key [this key val]
    (.put db (key->bytes key) (val->bytes val))
    (clear [bkey bval]))

  (get-key [this key]
    (with-open [txn (.txnRead env)]
      (if-let [val (.get db txn (key->bytes key))]
        (let [decoded (str (.decode (StandardCharsets/UTF_8) val))]
          (clear [bkey bval])
          decoded)
        (clear [bkey bval]))))

  (del-key [this key]
    (.delete db (key->bytes key))
    (.clear bkey)
    nil)

  (close [this]
    (.close env)))

(deftype Lmdb []
  AStorage
  (open-conn [this db]
    (let [env (-> (Env/create)
                  (.setMapSize 10485760)
                  (.setMaxDbs 5)
                  (.open (.-file db) nil))
          db (.openDbi env
                       (.-name db)
                       (into-array DbiFlags [DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT]))]
      (->LmdbConnection env db))))


(def db (->Database "prova" (File. ".") (->Lmdb)))
(def mconn (conn db))