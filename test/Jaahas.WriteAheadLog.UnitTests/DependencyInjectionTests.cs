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
    public async Task ShouldRegisterWriteAheadLog() {
        var builder = Host.CreateApplicationBuilder();
        builder.Services
            .AddWriteAheadLogServices()
            .AddFile(TestContext.TestName!, options => {
                options.DataDirectory = Path.Combine(s_tempPath, TestContext.TestName!);
            });
        
        using var app = builder.Build();
        
        var log = app.Services.GetRequiredKeyedService<IWriteAheadLog>(TestContext.TestName!);
        await log.InitAsync(TestContext.CancellationTokenSource.Token);

        await log.WriteAsync(Enumerable.Repeat((byte) 1, 100).ToArray(), TestContext.CancellationTokenSource.Token);
    }

}
