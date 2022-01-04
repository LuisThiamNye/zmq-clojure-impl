(ns crypticbutter.zmq.impl.util
  (:import
   (java.util.concurrent.locks ReentrantLock)))

(defn set-conj [coll x]
  (if (nil? coll)
    #{x}
    (conj coll x)))

(defmacro with-lock [^ReentrantLock lock & body]
  `(do (.lock ~lock)
       (try
         ~@body
         (finally (.unlock ~lock)))))
