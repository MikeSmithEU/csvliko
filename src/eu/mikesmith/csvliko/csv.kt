package eu.mikesmith.csvliko

import java.io.ByteArrayOutputStream
import java.io.InputStream

class UnknownDialectException(dialect: String) : Exception("Unknown dialect '$dialect'")

open class CSVParserException : Exception {
    constructor(message: String, char: Char, line: Long, column: Long, byte: Long) :
            super("Unexpected '$char'. $message (on line $line, column $column, byte $byte)")

    constructor(message: String, line: Long, column: Long, byte: Long) :
            super("$message (on line $line, column $column, byte $byte)")
}

class UnclosedQuoteException(message: String, line: Long, column: Long, byte: Long) :
    CSVParserException(message, line, column, byte)

class UnallowedQuoteException(message: String, char: Char, line: Long, column: Long, byte: Long) :
    CSVParserException(message, char, line, column, byte)


data class Dialect(
    val delimiters: List<Int>,
    val quoteChars: List<Int>,
    val commentChars: List<Int>,
    val trimCharsStart: List<Int>,
    val trimCharsEnd: List<Int>,
    val ignoreEmptyLines: Boolean
) {
    companion object {
        private fun stringToIntList(str: String): List<Int> {
            return str.toByteArray(Charsets.UTF_8).map { it.toInt() }
        }

        fun fromStrings(
            delimiters: String,
            quoteChars: String,
            commentChars: String,
            trimCharsStart: String,
            trimCharsEnd: String,
            ignoreEmptyLines: Boolean
        ): Dialect {
            return Dialect(
                stringToIntList(delimiters),
                stringToIntList(quoteChars),
                stringToIntList(commentChars),
                stringToIntList(trimCharsStart),
                stringToIntList(trimCharsEnd),
                ignoreEmptyLines
            )
        }

        fun fromStrings(
            delimiters: String,
            quoteChars: String,
            commentChars: String,
            trimChars: String,
            ignoreEmptyLines: Boolean
        ): Dialect {
            return Dialect(
                stringToIntList(delimiters),
                stringToIntList(quoteChars),
                stringToIntList(commentChars),
                stringToIntList(trimChars),
                stringToIntList(trimChars),
                ignoreEmptyLines
            )
        }
    }
}

val DefaultDialect = Dialect.fromStrings(
    ",",
    "\"",
    "#",
    " \t\n\r",
    true
)

val WhitespaceDialect = Dialect.fromStrings(
    " \t",
    "\"",
    "#",
    " \t\n\r",
    true
)

val knownDialects = mapOf(
    "default" to DefaultDialect,
    "whitespace" to WhitespaceDialect
)

// Factory for using a string as dialect
fun Reader(stream: InputStream, dialect: String): Reader {
    return knownDialects[dialect]?.let {
        Reader(stream, it)
    } ?: run {
        throw UnknownDialectException(dialect)
    }
}

/**
 * Reads a csv-like InputStream according to dialect and returns one row at a time...
 *
 */
class Reader(stream: InputStream, dialect: Dialect = DefaultDialect) : Iterator<List<String>> {
    private var internalReader: InternalReader = InternalReader(stream, dialect)
    private var curItem: List<String>? = null
    private var curException: Throwable? = null

    init {
        // pre-fetch one "line" so we can determine hasNext()
        fetchNext()
    }

    private fun fetchNext() {
        internalReader
            .runCatching {
                curItem = this.next()
            }
            .onFailure {
                curException = it
                curItem = null
            }
    }

    override fun hasNext(): Boolean {
        return curItem != null
    }

    override fun next(): List<String> {
        return curException?.let {
            throw it
        } ?: run {
            val result = curItem ?: run {
                throw NoSuchElementException("Requested next item after iteration completed")
            }
            fetchNext()
            result
        }
    }
}

internal class InternalReader(private val stream: InputStream, private val dialect: Dialect = DefaultDialect) {
    private var mode: Byte = 0

    private var currentLinePosition = 1L
    private var currentCharPosition = 0L
    private var currentBytePosition = 0L
    private var lastQuoteLinePosition = 0L
    private var lastQuoteCharPosition = 0L
    private var lastQuoteBytePosition = 0L

    private var lastQuoteKind = 0

    private var delimiterIsWhitespace: Boolean = false
    private var whitespaceChars: CharArray = CharArray(0)

    private var line: MutableList<String> = ArrayList()
    private var field: ByteArrayOutputStream = ByteArrayOutputStream()

    init {
        delimiterIsWhitespace = dialect.delimiters.any { it in dialect.trimCharsEnd }
        whitespaceChars = dialect.trimCharsEnd.toString().toCharArray()
    }

    private fun nextField() {
        var strField: String = field.toString()
        field = ByteArrayOutputStream()

        if (mode != MODE_INSIDE_QUOTED_QUOTE) {
            strField = trimEnd(strField)
        }

        line.add(strField)

        mode = MODE_OUTSIDE
    }

    private fun yieldLine(new_mode: Byte = 0): List<String> {
        if (mode != MODE_OUTSIDE || !delimiterIsWhitespace) {
            nextField()
        }

        field.reset()
        mode = new_mode

        val result: List<String> = line.toList()
        line.clear()
        return result
    }

    private fun trimEnd(data: String): String {
        return data.trimEnd(*whitespaceChars)
    }

    private fun isIgnoreStart(byte: Int): Boolean {
        return byte in dialect.trimCharsStart
    }

    private fun isIgnoreEnd(byte: Int): Boolean {
        return byte in dialect.trimCharsEnd
    }

    private fun isComment(byte: Int): Boolean {
        return byte in dialect.commentChars
    }

    private fun isQuote(byte: Int): Boolean {
        if (lastQuoteKind > 0) {
            return byte == lastQuoteKind
        }
        return byte in dialect.quoteChars
    }

    private fun isDelimiter(byte: Int): Boolean {
        return byte in dialect.delimiters
    }

    fun next(): List<String>? {
        var byte: Int
        var isNewLine: Boolean

        while (true) {
            byte = stream.read()
            if (byte < 0) {
                break
            }

            currentBytePosition++
            isNewLine = byte == 0xa || byte == 0xd

            if (isNewLine) {
                currentCharPosition = 0
                currentLinePosition++
            } else {
                currentCharPosition++
            }

            when (mode) {
                MODE_COMMENT -> {
                    if (isNewLine) {
                        mode = MODE_FIRST
                    }
                }

                MODE_OUTSIDE, MODE_FIRST -> {
                    if (isNewLine) {
                        if (mode != MODE_FIRST) {
                            return yieldLine()
                        }
                        continue
                    }

                    if (isIgnoreStart(byte)) {
                        continue
                    }

                    if (isComment(byte)) {
                        if (mode == MODE_OUTSIDE) {
                            return yieldLine()
                        }
                        mode = MODE_COMMENT
                        continue
                    }

                    if (isQuote(byte)) {
                        lastQuoteKind = byte
                        mode = MODE_INSIDE_QUOTED
                        lastQuoteLinePosition = currentLinePosition
                        lastQuoteCharPosition = currentCharPosition
                        lastQuoteBytePosition = currentBytePosition
                        continue
                    }

                    if (isDelimiter(byte)) {
                        nextField()
                        continue
                    }

                    mode = MODE_INSIDE
                    field.write(byte)
                    continue
                }

                MODE_INSIDE -> {
                    if (isQuote(byte)) {
                        throw UnallowedQuoteException(
                            "Quote not allowed here",
                            byte.toChar(),
                            currentLinePosition,
                            currentCharPosition,
                            currentBytePosition
                        )
                    }

                    if (isNewLine) {
                        return yieldLine()
                    }

                    if (isDelimiter(byte)) {
                        nextField()
                        continue
                    }

                    field.write(byte)
                    continue
                }

                MODE_INSIDE_QUOTED_QUOTE -> {
                    if (byte == lastQuoteKind) {
                        field.write(byte)
                        mode = MODE_INSIDE_QUOTED
                        continue
                    }

                    if (isDelimiter(byte)) {
                        nextField()
                        continue
                    }

                    if (isNewLine) {
                        return yieldLine()
                    }

                    if (!delimiterIsWhitespace) {
                        if (isIgnoreEnd(byte)) {
                            continue
                        }

                        if (isComment(byte)) {
                            return yieldLine(MODE_COMMENT)
                        }
                    }

                    throw UnallowedQuoteException(
                        "Single quote inside quoted field",
                        byte.toChar(),
                        currentLinePosition,
                        currentCharPosition,
                        currentBytePosition
                    )
                }

                MODE_INSIDE_QUOTED -> {
                    if (byte == lastQuoteKind) {
                        mode = MODE_INSIDE_QUOTED_QUOTE
                        continue
                    }

                    field.write(byte)
                    continue
                }
            }
        }

        return when (mode) {
            MODE_INSIDE_QUOTED_QUOTE, MODE_OUTSIDE, MODE_INSIDE -> {
                yieldLine()
            }

            MODE_INSIDE_QUOTED -> {
                throw UnclosedQuoteException(
                    "Unexpected end",
                    lastQuoteLinePosition,
                    lastQuoteCharPosition,
                    lastQuoteBytePosition
                )
            }

            else -> {
                null
            }
        }
    }

    companion object {
        private const val MODE_FIRST: Byte = 0
        private const val MODE_OUTSIDE: Byte = 1
        private const val MODE_INSIDE: Byte = 2
        private const val MODE_INSIDE_QUOTED: Byte = 3
        private const val MODE_INSIDE_QUOTED_QUOTE: Byte = 4
        private const val MODE_COMMENT: Byte = 5
    }
}