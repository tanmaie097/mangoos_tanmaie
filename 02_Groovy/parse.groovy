import groovy.json.JsonSlurper

// ------ Read input -------
def inputFile = new File("/Users/tanmaie/Desktop/magnoos/exercises/02_Groovy/input.json")
def jsonText = inputFile.text

// ------ Parse JSON -------
def json = new JsonSlurper().parseText(jsonText)

// We assume the JSON is a list of maps, like: [ {..}, {..} ]
// Get headers from the first object
def headers = json[0].keySet().toList()

// ------ Write CSV -------
def outputFile = new File("output.csv")

outputFile.withWriter { writer ->

    // Write header row
    writer.writeLine(headers.join(","))

    // Write each row
    json.each { row ->
        def values = headers.collect { h -> row[h] }
        writer.writeLine(values.join(","))
    }
}

println "CSV created: output.csv"
