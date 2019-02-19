(ns ldb.storage
  (:require [clojure.reflect :refer [reflect]])
  (:import (sun.misc Unsafe)
           (sun.nio.ch FileChannelImpl)
           (java.io RandomAccessFile)))

(defn- get-java-method
  [clazz name & args]
  (doto
    (.getDeclaredMethod clazz name (into-array Class args))
    (.setAccessible true)))

(def unsafe (-> (doto
                  (.getDeclaredField Unsafe "theUnsafe")
                  (.setAccessible true))
                (.get nil)))
(def byte-array-offset (.arrayBaseOffset unsafe (Class/forName "[B")))
(def map0 (get-java-method FileChannelImpl "map0" Integer/TYPE Long/TYPE Long/TYPE))
(def unmap0 (get-java-method FileChannelImpl "unmap0" Long/TYPE Long/TYPE))

(defn mmap
  [^String loc ^Long size]
  (let [size (bit-and (+ size 0xfff) (bit-not 0xfff))]
    (with-open [bf (RandomAccessFile. loc "r")]
      (with-open [ch (.getChannel bf)]
        (long (.invoke map0 ch (into-array Object [(int 0) (long 0) (long size)])))))))

(defn open-file
  [^String loc]
  (let [bf (RandomAccessFile. loc "rw")]
    (.getChannel bf)))

(defn read-bytes
  [addr offset length]
  (let [data (byte-array length)]
    (.copyMemory unsafe nil (+ addr offset) data byte-array-offset length)
    data))

(defn write-bytes
  [addr offset data]
  (.copyMemory unsafe data byte-array-offset nil (+ addr offset) (count data)))

(comment
  (clojure.java.shell/sh "cat" "tmp")

  (def addr (mmap "tmp" 0 0 10))

  (String. (read-bytes addr 0 10))

  (clojure.java.shell/sh "cat" "tmp"))