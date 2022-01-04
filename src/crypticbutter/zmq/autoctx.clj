(ns crypticbutter.zmq.autoctx
  (:require
   [clojure.walk :as walk]
   [farolero.core :refer [error]]
   [crypticbutter.zmq :as zmq]))

(def ^:const sugar-ns "crypticbutter.zmq.autoctx")
(def ^:const core-ns "crypticbutter.zmq")

(defmacro with-context [ctx & body]
  (list* `do
         (mapv
          (fn [bodypart]
            (walk/postwalk
             (fn [form]
               (or
                (when-some [fsym (and (list? form) (first form))]
                  (when (and (symbol? fsym)
                             (= sugar-ns (some-> (resolve fsym) namespace)))
                    (list* (symbol core-ns (name fsym))
                           ctx
                           (rest form))))
                form))
             (macroexpand bodypart)))
          body)))

(defmacro with-new-context [& body]
  `(let [ctx# (zmq/create-context)
         res# (with-context ctx# ~@body)]
     (zmq/terminate-context! ctx#)
     res#))

(defn- stuberror []
  (error :error/incorrect
         {:message "Function must be called inside `with-context` and be statically expressed."}))

;;
;; These functions are swapped out with the counterparts with an extra ctx arg
;;

(defn socket
  {:arglists '([sock-type options] [sock-type])}
  ([_ _] (stuberror))
  ([_] (stuberror)))

(defn bind
  [socket endpoint]
  (stuberror))

(defn connect
  [socket endpoint]
  (stuberror))

(defn disconnect
  [socket]
  (stuberror))

(defn send
  [socket msg options]
  (stuberror))

(defn recv
  [socket options]
  (stuberror))
