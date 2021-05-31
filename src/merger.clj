(ns merger
  (:require [clojure.data.xml :as xml]
            [clojure.data.xml.node :refer [element]]
            [clojure.java.io :as io]
            [clojure.string :as string])
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

#_(string/split-lines (slurp "/home/kostas/temp/note1.xml"))

#_(first (string/split-lines (slurp "/home/kostas/projects/clojure/xml-merger/temp/dataset/KR20140109/KR/20140109/B1/000101/35/01/03/KR-101350103-B1.xml")))

(def xml-preamble "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
(def patents-opening "<patent-documents>")
(def patents-closing "</patent-documents>")

(defn merge-and-write-as-txt [out-dir patent-id files]
  (let [out-file (str out-dir "/" patent-id ".xml")]
    (if (> (count files) 1)
      (let [
            sb (StringBuilder. xml-preamble)
            txts (->>
                   (map slurp files)
                   (map string/split-lines)
                   (map next))]
        (.append sb patents-opening)
        (doseq [ts txts]
          (doseq [t ts]
            (.append sb t)))
        (.append sb patents-closing)
        (spit out-file (.toString sb)))
      (io/copy (java.io.File. (first files)) (java.io.File. out-file)))))

(comment
  (let [fs ["/home/kostas/temp/note1.xml" "/home/kostas/temp/note2.xml"]]
    (merge-and-write-as-txt "/home/kostas/temp" "as-txt" fs)))

;; TODO OS agnostic sep
(defn merge-and-write [out-dir patent-id files]
  (let [f (str out-dir "/" patent-id ".xml")]
    (with-open [out-file (FileWriter. f)]
      #_(println xmls)
      (xml/emit (element :patent-documents nil (map parse-file files)) out-file))))