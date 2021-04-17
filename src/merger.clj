(ns merger
  (:require [clojure.data.xml :as xml]
            [clojure.data.xml.node :refer [element]]
            [clojure.java.io :as io])
  (:import (java.io FileWriter)))

(defn parse-file [f]
  (xml/parse (io/input-stream (io/file f))))

#_(xml/parse (io/input-stream (io/file "/home/kostas/temp/note1.xml")))

#_(xml/parse (java.io.StringReader. (slurp "/home/kostas/temp/note1.xml")))

#_(def n1 (parse-file "/home/kostas/temp/note1.xml"))
#_(def n2 (parse-file "/home/kostas/temp/note2.xml"))

#_(println (element :notes nil [n1 n2]))

#_(xml/emit m)

#_(with-open [out-file (FileWriter. "/home/kostas/temp/notes-merged.xml")]
  (xml/indent (element :notes nil [n1 n2]) out-file))


;; TODO OS agnostic sep
(defn merge-and-write [out-dir patent-id files]
  (let [f (str out-dir "/" patent-id ".xml")]
    (with-open [out-file (FileWriter. f)]
      #_(println xmls)
      (xml/emit (element :patent-documents nil (map parse-file files)) out-file))))