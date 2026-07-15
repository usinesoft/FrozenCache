using PersistentStore;

namespace FrozenCache;

public class ServerSettings
{
    public int Port { get; set; }

    /// <summary>
    /// The dictionary index is slightly faster for lookups, but uses more memory. The ordered index is slower for lookups, but uses less memory.
    /// </summary>
    public IndexType PrimaryIndexType { get; set; } = IndexType.Dictionary;

    /// <summary>
    /// When enabled, every accepted TCP connection is upgraded to TLS using <see cref="SslCertificatePath"/>
    /// before any protocol message is read. Clients must then connect with SSL enabled too.
    /// </summary>
    public bool UseSsl { get; set; }

    /// <summary>
    /// Path to a PFX (PKCS#12) file containing the server certificate and its private key. Required when
    /// <see cref="UseSsl"/> is true.
    /// </summary>
    public string? SslCertificatePath { get; set; }

    /// <summary>
    /// Password protecting <see cref="SslCertificatePath"/>, if any.
    /// </summary>
    public string? SslCertificatePassword { get; set; }
}




