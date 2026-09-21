using IntegrationHost.Pipes;

namespace IntegrationHost.Tests;

public class PipeValidatorTests
{
    private static PipeConfig Good(string name = "audit") => new()
    {
        Name = name,
        Source = new() { Transport = "redis", Stream = "audit", ConsumerGroup = "int-audit" },
        Destination = new() { Transport = "http", Url = "https://partner.example/hook" },
    };

    [Fact]
    public void A_good_pipe_has_no_errors() =>
        Assert.Empty(PipeValidator.Validate([Good()], new() { RedisConnectionString = "localhost" }));

    [Fact]
    public void Every_problem_is_reported_not_just_the_first_and_names_the_pipe()
    {
        var bad = Good("bad");
        bad.Source.Stream = "";
        bad.Source.ConsumerGroup = "";
        bad.Destination.Url = "not a url";
        bad.OnFailure = "explode";

        var errors = PipeValidator.Validate([bad]);

        Assert.Equal(4, errors.Count);
        Assert.All(errors, e => Assert.StartsWith("Pipe 'bad':", e));
    }

    [Fact]
    public void Duplicate_names_are_rejected() =>
        Assert.Contains(PipeValidator.Validate([Good("a"), Good("A")]), e => e.Contains("not unique"));

    [Theory]
    [InlineData("kafka")]
    [InlineData("")]
    public void Unsupported_transports_are_named(string transport)
    {
        var p = Good();
        p.Source.Transport = transport;
        p.Destination.Transport = transport;
        var errors = PipeValidator.Validate([p]);
        Assert.Contains(errors, e => e.Contains("Source.Transport"));
        Assert.Contains(errors, e => e.Contains("Destination.Transport"));
    }

    [Fact]
    public void Handler_maps_are_rejected_until_supported()
    {
        var p = Good();
        p.Map = new() { Handler = "AuditToSiem" };
        Assert.Contains(PipeValidator.Validate([p]), e => e.Contains("Map.Handler"));
    }

    [Fact]
    public void A_template_map_needs_a_rule_engine_url_and_a_valid_no_match_mode()
    {
        var p = Good();
        p.Map = new() { Template = "audit-siem-v1" };
        Assert.Contains(PipeValidator.Validate([p], new() { RedisConnectionString = "x" }), e => e.Contains("RuleEngineUrl"));
        Assert.Empty(PipeValidator.Validate([p], new() { RedisConnectionString = "x", RuleEngineUrl = "http://rules:8080" }));

        p.Map.OnNoMatch = "shrug";
        Assert.Contains(PipeValidator.Validate([p], new() { RedisConnectionString = "x", RuleEngineUrl = "http://rules:8080" }), e => e.Contains("OnNoMatch"));
        p.Map = new();
        Assert.Contains(PipeValidator.Validate([p]), e => e.Contains("names no Template"));
    }

    [Fact]
    public void A_disabled_pipe_is_still_validated()
    {
        var p = Good();
        p.Enabled = false;
        p.Destination.Url = "nope";
        Assert.NotEmpty(PipeValidator.Validate([p]));
    }

    [Fact]
    public void Enabled_redis_pipe_needs_a_connection_string_but_disabled_does_not()
    {
        var p = Good();
        Assert.Contains(PipeValidator.Validate([p], new()), e => e.Contains("RedisConnectionString"));
        p.Enabled = false;
        Assert.Empty(PipeValidator.Validate([p], new()));
    }

    [Theory]
    [InlineData("block")]
    [InlineData("skip-and-alert")]
    [InlineData("SkipAndAlert")]
    public void Failure_policy_accepts_both_spellings(string value)
    {
        var p = Good();
        p.OnFailure = value;
        Assert.Empty(PipeValidator.Validate([p]));
    }

    [Fact]
    public void ThrowIfInvalid_lists_all_errors_in_one_exception()
    {
        var p = Good();
        p.Source.Stream = "";
        p.Destination.Url = "x";
        var ex = Assert.Throws<PipeConfigException>(() => PipeValidator.ThrowIfInvalid([p]));
        Assert.Contains("Source.Stream", ex.Message);
        Assert.Contains("Destination.Url", ex.Message);
    }
}

public class EventHubsValidationTests
{
    private static PipeConfig Pipe(Action<PipeConfig> tweak)
    {
        var p = new PipeConfig
        {
            Name = "eh",
            Source = new() { Transport = "eventhubs", ConnectionString = "Endpoint=sb://x", EventHub = "h", ConsumerGroup = "$Default", CheckpointConnectionString = "UseDevelopmentStorage=true" },
            Destination = new() { Transport = "eventhubs", Namespace = "ns.servicebus.windows.net", EventHub = "out" },
        };
        tweak(p);
        return p;
    }

    [Fact]
    public void A_complete_eventhubs_pipe_is_valid() => Assert.Empty(PipeValidator.Validate([Pipe(_ => { })], new()));

    [Fact]
    public void Source_needs_exactly_one_credential_a_hub_and_a_checkpoint_store()
    {
        var errors = PipeValidator.Validate([Pipe(p =>
        {
            p.Source.Namespace = "ns"; // both connection string and namespace
            p.Source.EventHub = "";
            p.Source.CheckpointConnectionString = "";
        })]);
        Assert.Contains(errors, e => e.Contains("Source.EventHub"));
        Assert.Contains(errors, e => e.Contains("Source needs exactly one of ConnectionString or Namespace"));
        Assert.Contains(errors, e => e.Contains("Checkpoint"));
    }

    [Fact]
    public void Destination_needs_a_hub_and_exactly_one_credential()
    {
        var errors = PipeValidator.Validate([Pipe(p => { p.Destination.EventHub = ""; p.Destination.Namespace = ""; })]);
        Assert.Contains(errors, e => e.Contains("Destination.EventHub"));
        Assert.Contains(errors, e => e.Contains("Destination needs exactly one"));
    }

    [Fact]
    public void Redis_destination_needs_a_stream_and_a_connection_string()
    {
        var p = Pipe(p => p.Destination = new() { Transport = "redis" });
        var errors = PipeValidator.Validate([p], new());
        Assert.Contains(errors, e => e.Contains("Destination.Stream"));
        Assert.Contains(errors, e => e.Contains("Host:RedisConnectionString"));
    }
}
