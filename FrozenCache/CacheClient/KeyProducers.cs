namespace CacheClient;

/// <summary>
/// Convert different types to long to be used as keys
/// </summary>
public static class KeyProducers
{

    public static readonly int PrecisionFactor = 10_000;

    public static long FromNull()
    {
        return long.MinValue;
    }

    /// <summary>
    /// To be able to compare integers anf float values all numeric keys are multiplied by a precision factor
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static long FromInt(this int value)
    {
        return value * PrecisionFactor;
    }

    public static long FromDouble(this double value)
    {
        return (long)(value * PrecisionFactor);
    }

    public static long FromFloat(this float value)
    {
        return (long)(value * PrecisionFactor);
    }

    public static long FromLong(this long value)
    {
        return value * PrecisionFactor;
    }
    public static long FromDecimal(this decimal value)
    {
        return (long)(value * PrecisionFactor);
    }


    /// <summary>
    /// To have the most discriminant key for a string we use a double encoding; the least significant bits are the hash code of the string,
    /// In the most significant bits we store the first 5 characters of the string
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException"></exception>
    public static long FromString(this string? value)
    {
        if (value == null) return FromNull();

        var hash = value.GetHashCode();
        var len = Math.Min(5, value.Length);
        var first = value.Substring(0, len).EncodeString();

        first <<= 32; // shift the first 5 characters to the most significant bits
        return first | (uint)hash; // combine the two parts, the first 5 characters and the hash code
    }


    /// <summary>
    /// Encode a char in a 6 bit value
    /// </summary>
    /// <param name="ch"></param>
    /// <returns></returns>
    private static int EncodeChar(char ch)
    {
        if(ch is >= 'a' and <= 'z')
            return ch - 'a';

        // case-insensitive encoding
        if (ch is >= 'A' and <= 'Z')
            return ch - 'A';

        const int digitOffset = 'z'-'a' + 1;

        if (ch is >= '0' and <= '9')
            return ch - '0' + digitOffset;

        const int otherOffset = digitOffset + 10;
        if(ch== '_')
            return otherOffset + 1;
        if (ch == '-')
            return otherOffset + 2;
        if (ch == '.')
            return otherOffset + 3;

        return otherOffset + 4; // for any other character, we use a special value
    }

    public static long EncodeString(this string value)
    {
        // a long may contain maximum 10 characters, so we can encode it in 60 bits
        var len = Math.Min(10, value.Length);

        long result = 0;

        for (int i = 0; i < len; i++)
        {
            var ch = value[i];
            result |= (long)EncodeChar(ch) << (i * 6);
        }

        return result;

    }


    public static long FromDateTime(this DateTime value)
    {
        return value.ToUniversalTime().Ticks;
    }
    public static long FromDateTimeOffset(this DateTimeOffset value)
    {
        return value.ToUniversalTime().Ticks;
    }

    public static long FromDateOnly(this DateOnly value)
    {
        return value.ToDateTime(TimeOnly.MinValue).Ticks;
    }
}