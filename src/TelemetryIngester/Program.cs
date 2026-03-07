using Microsoft.Extensions.Options;
using Serilog;
using TelemetryIngester.Configuration;
using TelemetryIngester.Kafka;
using TelemetryIngester.Mapping;
using TelemetryIngester.Services;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSerilog((_, loggerConfig) =>
    loggerConfig.ReadFrom.Configuration(builder.Configuration));

builder.Services.Configure<TelemetryOptions>(builder.Configuration.GetSection("Telemetry"));
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<TimescaleDbOptions>(builder.Configuration.GetSection("TimescaleDb"));
builder.Services.Configure<IngesterOptions>(builder.Configuration.GetSection("Ingester"));

builder.Services.AddSingleton<IPacketMapper, PacketMapper>();
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();

builder.Services.AddHostedService<UdpListenerService>();

var host = builder.Build();
host.Run();
