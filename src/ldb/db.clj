(ns ldb.db
  (:require [clojure.data.fressian :as fress]
            [clojure.spec.alpha :as s]
            [clojure.set])
  (:import (org.lmdbjava Env DbiFlags PutFlags KeyRange CursorIterator Dbi Txn)
           (java.io File)
           (java.nio ByteBuffer ByteOrder)
           (clojure.lang IReduceInit Named)
           (java.util UUID HashMap Date)
           (java.time Instant)
           (net.openhft.hashing LongHashFunction)))

(defrecord Connection [^Dbi eavt ^Dbi eavt-history
                       ^Dbi aevt ^Dbi aevt-history
                       ^Dbi avet ^Dbi avet-history
                       ^Dbi vaet ^Dbi vaet-history
                       ^Env env ^Dbi status ^Dbi ident
                       ^Dbi log])

;; specs
(def value-types #{:db.type/keyword
                   :db.type/string
                   :db.type/boolean
                   :db.type/long
                   :db.type/bigint
                   :db.type/float
                   :db.type/double
                   :db.type/ref
                   :db.type/instant
                   :db.type/uuid
                   :db.type/bytes})

(s/def :db/ident keyword?)
(s/def :db/id (s/or :id int?
                    :tempid string?))
(s/def :db/valueType value-types)
(s/def :db/cardinality #{:db.cardinality/one
                         :db.cardinality/many})
(s/def ::schema (s/keys :req [:db/ident :db/valueType :db/cardinality]))

;; const

(def schema-idents (clojure.set/union
                     #{:tx/txInstant}
                     #{:db/ident :db/valueType :db/cardinality}
                     #{:db.cardinality/one :db.cardinality/many}
                     value-types))
;;

(defn encode-key
  [data]
  (if (int? data)
    (let [bkey (ByteBuffer/allocateDirect 8)]
      (.order bkey (ByteOrder/BIG_ENDIAN))
      (.putLong bkey data)
      (.flip bkey))
    (let [data (fress/write data)
          bkey (ByteBuffer/allocateDirect (.limit data))]
      (.order bkey (ByteOrder/BIG_ENDIAN))
      (.put bkey data)
      (.flip bkey))))

(defn encode-val
  [data]
  (let [data (fress/write data)
        bkey (ByteBuffer/allocateDirect (.limit data))]
    (.order bkey (ByteOrder/BIG_ENDIAN))
    (.put bkey data)
    (.flip bkey)))

(defn encode-kv
  [k v]
  (let [v (fress/write v)
        hash (.hashBytes (LongHashFunction/xx) v 0 (.limit v))
        bval (ByteBuffer/allocateDirect (.limit v))]
    (.order bval (ByteOrder/BIG_ENDIAN))
    (.put bval v)
    (.flip bval)
    (if (int? k)
      (let [bkey (doto (ByteBuffer/allocateDirect 16)
                   (.order (ByteOrder/BIG_ENDIAN))
                   (.putLong k)
                   (.putLong hash)
                   (.flip))]
        [bkey bval])
      (let [k (fress/write k)
            bkey (doto (ByteBuffer/allocateDirect (+ (.limit k) 8))
                   (.order (ByteOrder/BIG_ENDIAN))
                   (.put k)
                   (.putLong hash)
                   (.flip))]
        [bkey bval]))))

(defn decode
  [data]
  (fress/read data))

(defn decode-key
  [buffer int-key?]
  (if int-key?
    (let [n (.getLong buffer)]
      (.flip buffer)
      n)
    (decode buffer)))

(defn txn-read
  [^Connection conn]
  (.txnRead ^Env (.-env conn)))

(defn txn-write
  [^Connection conn]
  (.txnWrite ^Env (.-env conn)))

(defn txn-commit
  [^Txn txn]
  (.commit txn))

(defn put-key
  [^Dbi db ^Txn txn key val]
  (.put db txn (encode-key key) (encode-val val) (into-array PutFlags [])))

(defn put-key-hash
  [^Dbi db ^Txn txn key val]
  (let [[k v] (encode-kv key val)]
    (.put db txn k v (into-array PutFlags []))))

(defn get-key
  [^Dbi db ^Txn txn key]
  (when-let [v (.get db txn (encode-key key))]
    (decode v)))

(defn scan
  [^Dbi db ^Txn txn ^KeyRange range]
  (reify IReduceInit
    (reduce [this f init]
      (let [^CursorIterator it (.iterate db txn range)]
        (try
          (loop [state init]
            (if (reduced? state)
              @state
              (if (.hasNext it)
                (recur (f state (.next it)))
                state)))
          (finally
            (.close it)))))))

(defn scan-key
  [^Dbi db ^Txn txn key]
  (let [ekey (encode-key key)
        range (KeyRange/atLeast ekey)]
    (eduction
      (take-while (fn [kv]
                    (let [k (decode-key (.key kv) (int? key))]
                      (= key k))))
      (map #(decode (.val %))) (scan db txn range))))

(defn scan-all
  ([^Dbi db ^Txn txn]
   (scan-all db txn false))
  ([^Dbi db ^Txn txn int-key?]
   (letfn [(rf [kv]
             (let [k (decode-key (.key kv) int-key?)
                   v (decode (.val kv))]
               [k v]))]
     (eduction (map rf) (scan db txn (KeyRange/all))))))

(defn del-key
  [^Dbi db ^Txn txn key]
  (.delete db txn (encode-key key)))

(defn del-kv
  [^Dbi db ^Txn txn key val]
  (let [[k _] (encode-kv key val)]
    (.delete db txn k)))

(defn close
  [^Connection conn]
  (.close ^Env (.-env conn)))

(defn insert-ident
  [^Connection conn ^Txn txn key value]
  (put-key (.-ident conn) txn key value)
  (put-key (.-ident conn) txn (str value) key))

(defn from-ident
  [^Connection conn ^Txn txn key]
  (get-key (.-ident conn) txn (str key)))

(defn to-ident
  [^Connection conn ^Txn txn key]
  (get-key (.-ident conn) txn key))

(defn connect
  [^String filepath]
  (let [^Env env (-> (Env/create)
                     (.setMapSize 1099511627776)
                     (.setMaxDbs 11)
                     (.open (File. filepath) nil))]
    (letfn [(open-db [name]
              (.openDbi env name (into-array DbiFlags [DbiFlags/MDB_CREATE])))]
      (let [conn (map->Connection
                   {:env          env
                    :eavt         (open-db "eavt")
                    :eavt-history (open-db "eavt-history")
                    :aevt         (open-db "aevt")
                    :aevt-history (open-db "aevt-history")
                    :avet         (open-db "avet")
                    :avet-history (open-db "avet-history")
                    :vaet         (open-db "vaet")
                    :vaet-history (open-db "vaet-history")
                    :status       (open-db "status")
                    :ident        (open-db "ident")
                    :log          (open-db "log")})]
        (with-open [txn (txn-write conn)]
          (doseq [[i ident] (map-indexed vector schema-idents)]
            (insert-ident conn txn ident (- (inc i))))
          (txn-commit txn))
        conn))))
;;

(defn ^:dynamic entity-by-id
  ([^Connection conn eid]
   (when eid
     (with-open [txn (txn-read conn)]
       (entity-by-id conn txn eid))))
  ([^Connection conn ^Txn txn eid]
   (when eid
     (let [entity (reduce
                    (fn [acc [_ attr value _ _]]
                      (let [attr (from-ident conn txn attr)
                            m (acc attr)]
                        (cond
                          (coll? m)
                          (assoc acc attr (conj m value))

                          (nil? m)
                          (assoc acc attr value)

                          :else
                          (assoc acc attr #{m value})))) {} (scan-key (.-eavt conn) txn eid))]
       (when-not (empty? entity)
         (assoc entity :db/id eid))))))

(defn entities-by-ids [conn eids]
  (mapv (fn [[eid]] (entity-by-id conn eid)) eids))

(defn get-and-inc
  [^Connection conn ^Txn txn key]
  (let [eid (or (get-key (.-status conn) txn key) -1)]
    (put-key (.-status conn) txn key (inc eid))
    (inc eid)))

(defn valid-type?
  [valueType value]
  (case valueType
    :db.type/keyword (keyword? value)
    :db.type/string (string? value)
    :db.type/boolean (boolean? value)
    :db.type/long (int? value)
    :db.type/bigint (integer? value)
    :db.type/float (float? value)
    :db.type/double (double? value)
    :db.type/ref (map? value)
    :db.type/instant (instance? Instant value)
    :db.type/uuid (instance? UUID value)
    :db.type/bytes (bytes? value)

    false))

(defn valid-value?
  [{:db/keys [_ valueType cardinality]} value]
  (case cardinality
    :db.cardinality/one
    (valid-type? valueType value)

    :db.cardinality/many
    (and (coll? value)
         (every? (partial valid-type? valueType) value))))

(defn validate-value
  [value schema]
  (when-not (valid-value? schema value)
    (throw (ex-info "invalid value" {:value  value
                                     :schema schema}))))

(defn find-datom
  [^Connection conn ^Txn txn [eid attr value]]
  (let [xf (comp
             (filter (fn [[ceid cattr cvalue]] (and (= ceid eid) (= cattr attr) (= cvalue value))))
             (halt-when any?))]
    (transduce xf identity nil (scan-key (.-eavt conn) txn eid))))

(defn ^:dynamic gen-tempid [] (rand-int 99999))

(defn map->actions
  [^Connection conn ^Txn txn m]
  (when (:db/ident m)
    (when-let [ex (s/explain-data ::schema m)]
      (throw (ex-info "invalid schema" ex))))

  (let [get-eid (fn [m] (or (:db/id m)
                            (to-ident conn txn (:db/ident m))
                            (gen-tempid)))

        validate (fn [attr value]
                   (when-not (= :db/id attr)
                     (if-let [ident (to-ident conn txn attr)]
                       (when-not (neg? ident)
                         (let [schema (entity-by-id conn txn ident)]
                           (validate-value value schema)))
                       (throw (ex-info "ident unknown" {:attr attr})))))
        eid (get-eid m)
        xf (fn [[attr value]]
             (validate attr value)
             (cond
               (map? value)
               (let [new-eid (get-eid value)
                     datom [eid attr new-eid true]
                     value (assoc value :db/id new-eid)]
                 (eduction cat [[datom] (map->actions conn txn value)]))

               (and (coll? value)
                    (map? (first value)))
               (let [new-eids (mapv get-eid value)
                     datoms (mapv (fn [new-eid] [eid attr new-eid true]) new-eids)
                     values (mapcat (fn [value neid]
                                      (map->actions conn txn (assoc value :db/id neid)))
                                    value new-eids)]
                 (eduction cat [datoms values]))

               (coll? value)
               (eduction (map (fn [val] [eid attr val true])) value)

               :else
               [[eid attr value true]]))]
    (eduction (mapcat xf) m)))

(defn resolve-tempid
  [^Connection conn ^Txn txn tempids tempid]
  (if-let [id (.get tempids tempid)]
    id
    (let [eid (get-and-inc conn txn :last-eid)]
      (.put tempids tempid eid)
      eid)))

(defn action->datoms
  [^Connection conn ^Txn txn ^Integer tempids]
  (letfn [(load-schema [attr]
            (entity-by-id conn txn (to-ident conn txn attr)))

          (resolve-tempids [attr value]
            (let [{:db/keys [valueType]} (load-schema attr)]
              (case valueType
                :db.type/ref
                (if (string? value) (resolve-tempid conn txn tempids value) value)
                value)))

          (hydrate [[eid attr value tx op :as datom]]
            (let [value (resolve-tempids attr value)]
              (cond
                (int? eid)
                (if (entity-by-id conn txn eid)
                  [eid attr value tx op]
                  (throw (ex-info "entity not found" {:eid eid})))

                (string? eid)
                [(resolve-tempid conn txn tempids eid) attr value tx op]

                :default
                [(get-and-inc conn txn :last-eid) attr value tx op])))

          (filter-attr [[eid attr value]]
            (and (not= attr :db/id)
                 (not= value (get (entity-by-id conn txn eid) attr))))

          (update-ident [[eid attr value :as all]]
            (when (= :db/ident attr)
              (insert-ident conn txn value eid))
            all)

          (->datoms [[eid attr value op]]
            (let [ident (to-ident conn txn attr)
                  tid (resolve-tempid conn txn tempids "ldb.tx")]
              (if (neg? ident)
                (let [old-value (get (entity-by-id conn txn eid) attr)]
                  (if (and op old-value)
                    [[eid ident old-value tid false]
                     [eid ident value tid true]]
                    [[eid ident value tid op]]))

                (let [{:db/keys [cardinality]} (entity-by-id conn txn ident)]
                  (case cardinality
                    :db.cardinality/one
                    (let [old-value (get (entity-by-id conn txn eid) attr)]
                      (if (and op old-value)
                        [[eid ident old-value tid false]
                         [eid ident value tid true]]
                        [[eid ident value tid op]]))

                    :db.cardinality/many
                    (let [old-value (get (entity-by-id conn txn eid) attr)]
                      (when-not (and op (contains? old-value value))
                        [[eid ident value tid op]])))))))]

    (comp
      (map hydrate)
      (filter filter-attr)
      (map update-ident)
      (mapcat ->datoms))))

(defn vector->actions
  [conn txn [action eid attr value]]
  (case action
    :db.fn/retractEntity
    (if-let [entity (entity-by-id conn txn eid)]
      (eduction (filter (fn [[attr value]] [eid attr value false])) entity)
      (throw (ex-info "entity not found" {:eid eid})))

    :db/add
    [[eid attr value true]]

    :db/retract
    [[eid attr value false]]))

(defn update-indexes
  [^Connection conn ^Txn txn [eid attr value _ op :as datom]]
  (letfn [(ref? [attr]
            (if-not (neg? attr)
              (let [{:db/keys [valueType]} (entity-by-id conn txn attr)]
                (= valueType :db.type/ref))
              false))]
    (if op
      (do
        (put-key-hash (.-eavt conn) txn eid datom)
        (put-key-hash (.-aevt conn) txn attr datom)
        (when (ref? attr) (put-key-hash (.-vaet conn) txn value datom))
        [datom])

      (let [[reid rattr rvalue :as to-retract] (find-datom conn txn datom)]
        (put-key-hash (.-eavt-history conn) txn eid datom)
        (put-key-hash (.-aevt-history conn) txn attr datom)
        (put-key-hash (.-eavt-history conn) txn eid to-retract)
        (put-key-hash (.-aevt-history conn) txn attr to-retract)
        (del-kv (.-eavt conn) txn reid to-retract)
        (del-kv (.-aevt conn) txn rattr to-retract)
        (when (ref? attr)
          (put-key-hash (.-vaet-history conn) txn value datom)
          (put-key-hash (.-vaet-history conn) txn value to-retract)
          (del-kv (.-vaet conn) txn rvalue to-retract))
        [datom to-retract]))))

(defn insert->datoms
  [^Connection conn ^Txn txn ^Integer tx-data]
  (let [tempids (HashMap.)
        tx {:db/id        "ldb.tx"
            :tx/txInstant (Date.)}
        tx-data (conj tx-data tx)]
    (letfn [(rf [state input]
              (let [xs (cond
                         (map? input)
                         (map->actions conn txn input)

                         (vector? input)
                         (vector->actions conn txn input))

                    datoms (into (:tx-data state)
                                 (comp
                                   (action->datoms conn txn (:tempids state))
                                   (mapcat (partial update-indexes conn txn)))
                                 xs)]
                {:tempids (:tempids state)
                 :tx-data datoms}))]

      (let [res (reduce rf {:tempids tempids
                            :tx-data []} tx-data)
            tid (get (:tempids res) "ldb.tx")
            tx (entity-by-id conn txn tid)]
        (put-key (.-log conn) txn (.getTime (:tx/txInstant tx)) (assoc tx :tx/txData (:tx-data res)))
        res))))

(defn transact
  [^Connection conn data]
  (let [tx-data (:tx-data data)]
    (with-open [txn (txn-write conn)]
      (binding [entity-by-id (let [m (HashMap.)
                                   fun entity-by-id]
                               (fn [conn txn eid]
                                 (when eid
                                   (if-let [entity (.get m eid)]
                                     entity
                                     (let [entity (fun conn txn eid)]
                                       (.put m eid entity)
                                       entity)))))
                gen-tempid (let [counter (volatile! 0)]
                             (fn []
                               (str (vswap! counter inc))))]
        (let [result (insert->datoms conn txn tx-data)]
          (txn-commit txn)
          result)))))

(defn show-db
  [{:keys [eavt eavt-history aevt aevt-history vaet vaet-history status ident log] :as conn}]
  (with-open [txn (txn-read conn)]
    (letfn [(load-index [db]
              (into (sorted-map) (group-by first (eduction (map second) (scan-all db txn)))))]
      {:eavt   {:current (load-index eavt)
                :history (load-index eavt-history)}
       :aevt   {:current (load-index aevt)
                :history (load-index aevt-history)}
       :vaet   {:current (load-index vaet)
                :history (load-index vaet-history)}
       :log    (into [] (scan-all log txn true))
       :status (into [] (scan-all status txn))
       :ident  (into [] (scan-all ident txn))})))

;; query

(defn binding-var?
  [x]
  (when (instance? Named x)
    (.startsWith (name x) "?")))

(defn lvar?
  [x]
  (or (binding-var? x) (= x '_)))

(defn load-datoms
  [^Connection conn ^Txn txn [eid attr val]]
  (let [filter-attr (filter (fn [[_ dattr _]] (= dattr attr)))
        filter-val (filter (fn [[_ _ dval]] (= dval val)))]
    (cond
      (not (lvar? eid))
      (cond
        (and (not (lvar? attr))
             (not (lvar? val)))
        (eduction filter-attr
                  filter-val
                  (scan-key (.-eavt conn) txn eid))

        (not (lvar? attr))
        (eduction filter-attr
                  (scan-key (.-eavt conn) txn eid))

        (not (lvar? val))
        (eduction filter-val
                  (scan-key (.-eavt conn) txn eid))

        :else
        (scan-key (.-eavt conn) txn eid))

      (not (lvar? attr))
      (if-not (lvar? val)
        (eduction filter-attr
                  (scan-key (.-aevt conn) txn attr))
        (scan-key (.-aevt conn) txn attr))

      :else
      (if-not (lvar? val)
        (eduction
          (mapcat second)
          filter-val
          (scan-all (.-aevt conn) txn))

        (eduction
          (mapcat second)
          (scan-all (.-aevt conn) txn))))))

(defn match-datom
  [frame pattern datom]
  (loop [i 0
         frame frame]
    (if (= i (count pattern))
      frame
      (let [binding-var (nth pattern i)
            binded-value (get frame binding-var binding-var)
            datom-value (nth datom i)]
        (if (binding-var? binding-var)
          (recur (inc i) (assoc frame binding-var datom-value))
          (when (or (= '_ binded-value) (= datom-value binded-value))
            (recur (inc i) frame)))))))

(defn match-pattern
  [frame pattern]
  (comp
    (map (partial match-datom frame pattern))
    (filter (complement nil?))))

(defn update-pattern
  [conn txn frame [eid attr value]]
  (letfn [(unify [var] (get frame var var))]
    [(unify eid) (to-ident conn txn (unify attr)) (unify value)]))

(defn q
  ([^Connection conn data]
   (with-open [txn (txn-read conn)]
     (into [] (q conn txn data))))

  ([^Connection conn ^Txn txn {:keys [find where]}]
   (letfn [(xf [pattern]
             (mapcat (fn [frame]
                       (let [pattern (update-pattern conn txn frame pattern)
                             datoms (load-datoms conn txn pattern)]
                         (eduction
                           (filter (fn [[_ _ _ _ op]] op))
                           (match-pattern frame pattern) datoms)))))
           (substitute [frame]
             (eduction (map (partial get frame)) find))]
     (eduction (apply comp (mapv xf where))
               (map substitute) [{}]))))
