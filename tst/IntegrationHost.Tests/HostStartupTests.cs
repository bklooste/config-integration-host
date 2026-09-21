using System.Net;
using Microsoft.AspNetCore.Mvc.Testing;

namespace IntegrationHost.Tests;

public class HostStartupTests
{
    [Fact]
    public async Task With_no_pipes_the_host_is_healthy()
    {
        await using var factory = new WebApplicationFactory<Program>();
        using var client = factory.CreateClient();
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/health")).StatusCode);
    }

    [Fact]
    public void A_bad_pipe_fails_startup_and_the_message_lists_every_problem()
    {
        using var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("Pipes:0:Name", "bad");
            b.UseSetting("Pipes:0:Source:Transport", "redis");
            b.UseSetting("Pipes:0:Destination:Transport", "http");
            b.UseSetting("Pipes:0:Destination:Url", "nope");
        });

        var ex = Assert.Throws<Pipes.PipeConfigException>(() => factory.CreateClient());
        Assert.Contains("Source.Stream", ex.Message);
        Assert.Contains("Source.ConsumerGroup", ex.Message);
        Assert.Contains("Destination.Url", ex.Message);
    }

    [Fact]
    public void Bad_host_options_fail_fast_on_start()
    {
        using var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b => b.UseSetting("Host:PollIntervalMs", "1"));
        Assert.Throws<OptionsValidationException>(() => factory.CreateClient());
    }
}
