import eu.mikesmith.csvliko.Reader

fun main() {
    val testStrings: MutableList<String> = ArrayList()
    testStrings += """
        Field 1,    Field 2,  "Field 3"
        "A comma, I contain" # only one Field
           # This line will be ignored as it only contains a comment

        # Above empty line is ignored (as is this comment)
        "make sure to use ""double character escaping""..."

        # Even multi-line fields are supported:
        "- Line 1
        - Line2", second field

        # below is a single field which contains a single quote character,
        # padded with a single space on either side
        " "" "
    """.trimIndent()

    testStrings += "\n\rTe,st\n\"Line,-two\",   'hi'   , \"  ext  \" \n\"\"\n\n\n\n"
    testStrings += "\"\""
    testStrings += "\"\"\"\""
    testStrings += "\"\",\"test\",\" quiot\"\"\""
    testStrings += ""
    testStrings += "\n\n\n"
    testStrings += "\n\n\n h i \n\n\n"

    testStrings.forEach { csv ->
        val reader = Reader(csv.byteInputStream(Charsets.UTF_8))
        var i = 0L

        println("=".repeat(50))
        println(csv)
        println(". ".repeat(25))

        reader.forEach { line ->
            println("%4d | %s".format(i++, line.map { "'$it'" }))
        }

        println("=".repeat(50))
        println()
        println()
    }

    Reader("a\nb".byteInputStream()).run {
        this.forEach { _ -> }
        this.runCatching {
            this.next()
        }.onFailure {
            println("As expected, we have a proper exception '$it'")
        }.onSuccess {
            throw UnknownError("Excepted iterator to throw exception")
        }
    }
}
