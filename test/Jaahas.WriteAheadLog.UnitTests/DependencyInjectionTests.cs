using Jaahas.WriteAheadLog.DependencyInjection;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Jaahas.WriteAheadLog.UnitTests;

[TestClass]
public class DependencyInjectionTests {

    private static string s_tempPath = null!;
    
    public TestContext TestContext { get; set; } = null!;


    [ClassInitialize]
    public static void ClassInitialize(TestContext testContext) {
        s_tempPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        new DirectoryInfo(s_tempPath).Create();
    }


    [ClassCleanup]
    public static void ClassCleanup() {
        if (!string.IsNullOrWhiteSpace(s_tempPath) && Directory.Exists(s_tempPath)) {
            Directory.Delete(s_tempPath, true);
        }
    }


    [TestMethod]
    public async Task ShouldRegisterDefaultWriteAheadLog() {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddWriteAheadLog(options => {
            options.DataDirectory = Path.Combine(s_tempPath, TestContext.TestName!);
        });
        
        using var app = builder.Build();
        
        var log = app.Services.GetRequiredService<Log>();
        await log.InitAsync(TestContext.CancellationTokenSource.Token);

        using var msg = new LogMessage(Enumerable.Repeat((byte) 1, 100).ToArray());
        await log.WriteAsync(msg, TestContext.CancellationTokenSource.Token);
    }
    

    [TestMethod]
    public async Task ShouldRegisterNamedWriteAheadLog() {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddWriteAheadLog(TestContext.TestName!, options => {
            options.DataDirectory = Path.Combine(s_tempPath, TestContext.TestName!);
        });
        
        using var app = builder.Build();
        
        var log = app.Services.GetRequiredKeyedService<Log>(TestContext.TestName!);
        await log.InitAsync(TestContext.CancellationTokenSource.Token);

        using var msg = new LogMessage(Enumerable.Repeat((byte) 1, 100).ToArray());
        await log.WriteAsync(msg, TestContext.CancellationTokenSource.Token);
    }

}
