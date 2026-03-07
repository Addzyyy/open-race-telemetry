using Microsoft.Extensions.Options;
using Serilog;
using TelemetryIngester.Configuration;
using TelemetryIngester.Kafka;
using TelemetryIngester.Mapping;
using TelemetryIngester.Services;
using TelemetryIngester.Storage;

// Build the .NET generic host — this manages dependency injection, configuration,
// logging, and the application lifetime (start/stop signals).
var builder = Host.CreateApplicationBuilder(args);

// Replace the default Microsoft logging with Serilog, configured via appsettings.json.
builder.Services.AddSerilog((_, loggerConfig) =>
    loggerConfig.ReadFrom.Configuration(builder.Configuration));

// Bind each appsettings section to a strongly-typed options class.
// Services receive these via IOptions<T> constructor injection.
builder.Services.Configure<TelemetryOptions>(builder.Configuration.GetSection("Telemetry"));
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<TimescaleDbOptions>(builder.Configuration.GetSection("TimescaleDb"));
builder.Services.Configure<IngesterOptions>(builder.Configuration.GetSection("Ingester"));

// Register core services as singletons — one shared instance for the process lifetime.
builder.Services.AddSingleton<IPacketMapper, PacketMapper>();
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();

builder.Services.AddSingleton<ITimescaleWriter, TimescaleWriter>();

// Register hosted background services.
// The host starts them automatically and passes a cancellation token on shutdown.
builder.Services.AddHostedService<UdpListenerService>();
builder.Services.AddHostedService<KafkaConsumerService>();

var host = builder.Build();
host.Run();
