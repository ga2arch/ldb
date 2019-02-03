(ns db)

(defprotocol ADatom
  (to-eavt [this])
  (to-aevt [this]))

(deftype Datom [eid attr value tid op]
    Object
    (hashCode [this] ""))

(defprotocol AConnection
  (update-index [this name key datoms])
  (from-index [this name key])
  (append-log [this datoms]))

(defprotocol AStorage
  (conn [this options])
  (get-key [this key])
  (put-key [this key data])
  (append-key [this key data])
  (del-key [this key]))

(deftype LmdbConnection []
  AConnection
  (update-index [this name key datoms]
    []))

(deftype Lmdb [global-options]
  AStorage
  (conn [this conn-options] (->LmdbConnection)))


(defprotocol ADatabase
  (conn [this]))

(defrecord Database [options ^AStorage storage]
  ADatabase
  (conn [this]
    (conn storage options)))


(def db (->Database {} (->Lmdb {})))
(def conn (conn db))

(update-index conn nil nil [])