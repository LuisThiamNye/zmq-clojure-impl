(ns tasks
  (:require
   [babashka.tasks :refer [current-task run shell]]))

(defn shadow-cljs [& args]
  (apply shell "./node_modules/.bin/shadow-cljs" (map name args)))
