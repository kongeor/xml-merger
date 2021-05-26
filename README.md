# Xml merger

A program to merge xml patent files, organize them per region and to break them down into folders.


## Requirements

Java 8+

## Usage

Grab a built jar file from [releases](https://github.com/kongeor/xml-merger/releases).

Execute as follows:

```sh
java -jar xml-merger.jar -n 2000 --db-dir temp/db -o temp/output -p temp/dataset
```

Check all the cli options:

```sh
java -jar xml-merger.jar -h
```

## Implementation notes

### Combining xml files

All related patents will be merged into a single file with a common route.

This means that the following two documents:

```xml
<patent-document>
    <name>Foo</name>
</patent-document>
```

```xml
<patent-document>
    <name>Bar</name>
</patent-document>
```

Will be combined into:

```xml
<patent-documents>
    <patent-document>
        <name>Foo</name>
    </patent-document>
    <patent-document>
        <name>Bar</name>
    </patent-document>
</patent-documents>
```

**Note:** This also applies for single files, those will be nested under
`patent-documents` to maintain the same data shape as the others.

### Parallel execution

All the files per region will be processed in parallel using clojure's [pmap](https://clojuredocs.org/clojure.core/pmap).

This will drain all the cpu resources but has a huge gain in terms of execution time. On a recent
12 core i7, the sample dataset can be processed in ~10mins.
