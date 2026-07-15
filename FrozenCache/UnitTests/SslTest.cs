using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CacheClient;
using FrozenCache;
using Messages;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using PersistentStore;

namespace UnitTests;

/// <summary>
/// Verifies optional TLS support end to end: a real self-signed certificate is generated, the server is
/// started with SSL enabled, and a real <see cref="Connector"/> negotiates TLS to talk to it.
/// </summary>
public class SslTest
{
    private const string CertificatePassword = "test-password";

    private static string CreateSelfSignedPfx()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        var pfxBytes = cert.Export(X509ContentType.Pfx, CertificatePassword);

        var path = Path.Combine(Path.GetTempPath(), $"frozencache-test-{Guid.NewGuid()}.pfx");
        File.WriteAllBytes(path, pfxBytes);
        return path;
    }

    private static async Task<HostedTcpServer> StartSslServer(string certificatePath)
    {
        var logger = new Mock<ILogger<HostedTcpServer>>();
        var dataStore = new NullDataStore();

        var configuration = new Mock<IOptions<ServerSettings>>();
        configuration.Setup(x => x.Value).Returns(new ServerSettings
        {
            Port = 0, // dynamic port for tests
            UseSsl = true,
            SslCertificatePath = certificatePath,
            SslCertificatePassword = CertificatePassword
        });

        var server = new HostedTcpServer(dataStore, logger.Object, configuration.Object);
        await server.StartAsync(CancellationToken.None);

        await Task.Delay(500); // give the listener time to start

        return server;
    }

    [Test]
    public async Task FeedAndQueryOverSsl()
    {
        var certPath = CreateSelfSignedPfx();

        try
        {
            var server = await StartSslServer(certPath);

            try
            {
                // validation is disabled: the certificate is self-signed and not in a trusted chain
                using var client = new Connector("localhost", server.Port, useSsl: true, validateServerCertificate: false);

                Assert.That(client.Connect(), Is.True, "Client should connect and complete a TLS handshake");

                await client.CreateCollection("testCollection", "id");

                await client.FeedCollection("testCollection", "v1", GetItems(10));

                var result = await client.QueryByPrimaryKey("testCollection", 12);
                Assert.That(result, Is.Not.Null);
                Assert.That(result.Count, Is.EqualTo(1), "One object should be returned");
                Assert.That(result[0].Length, Is.EqualTo(121), "121 bytes of data were expected");
            }
            finally
            {
                await server.StopAsync(CancellationToken.None);
            }
        }
        finally
        {
            File.Delete(certPath);
        }
    }

    [Test]
    public async Task ConnectingWithCertificateValidationEnabledRejectsUntrustedCertificate()
    {
        var certPath = CreateSelfSignedPfx();

        try
        {
            var server = await StartSslServer(certPath);

            try
            {
                // validation is enabled (the default): a self-signed, untrusted certificate must be rejected
                using var client = new Connector("localhost", server.Port, useSsl: true, validateServerCertificate: true);

                Assert.That(client.Connect(), Is.False,
                    "Client should reject an untrusted self-signed certificate when validation is enabled");
            }
            finally
            {
                await server.StopAsync(CancellationToken.None);
            }
        }
        finally
        {
            File.Delete(certPath);
        }
    }

    [Test]
    public async Task SslClientConnectingToPlainServerGetsAnExplicitError()
    {
        var logger = new Mock<ILogger<HostedTcpServer>>();
        var dataStore = new NullDataStore();

        var configuration = new Mock<IOptions<ServerSettings>>();
        configuration.Setup(x => x.Value).Returns(new ServerSettings { Port = 0, UseSsl = false });

        var server = new HostedTcpServer(dataStore, logger.Object, configuration.Object);
        await server.StartAsync(CancellationToken.None);
        await Task.Delay(500);

        try
        {
            using var client = new Connector("localhost", server.Port, useSsl: true, validateServerCertificate: false);

            Assert.That(client.Connect(), Is.False, "An SSL client should fail to connect to a plain-text server");
            Assert.That(client.LastError, Does.Contain("SSL handshake"),
                "The error should explicitly call out the SSL handshake failure, not just 'connect failed'");
        }
        finally
        {
            await server.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task PlainClientConnectingToSslServerGetsAnExplicitError()
    {
        var certPath = CreateSelfSignedPfx();

        try
        {
            var server = await StartSslServer(certPath);

            try
            {
                // useSsl defaults to false here
                using var client = new Connector("localhost", server.Port);

                Assert.That(client.Connect(), Is.True,
                    "The plain TCP connect itself succeeds; the mismatch only surfaces at the protocol level");

                var ex = Assert.ThrowsAsync<CacheException>(async () =>
                    await client.CreateCollection("testCollection", "id"));

                Assert.That(ex!.Message, Does.Contain("SSL"),
                    "The error should explicitly call out the likely SSL mismatch, not just 'unexpected response'");
            }
            finally
            {
                await server.StopAsync(CancellationToken.None);
            }
        }
        finally
        {
            File.Delete(certPath);
        }
    }

    private static IEnumerable<Item> GetItems(int count)
    {
        for (var i = 0; i < count; i++)
            yield return new Item(new byte[100], i, i + 1);
    }
}
