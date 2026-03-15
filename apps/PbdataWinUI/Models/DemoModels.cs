using Microsoft.UI;
using Microsoft.UI.Xaml.Media;

namespace PbdataWinUI.Models;

public sealed class DemoStep
{
    public required int Number { get; init; }
    public required string PageKey { get; init; }
    public required string Title { get; init; }
    public required string ActionLabel { get; init; }
    public required string WhyItMatters { get; init; }
    public required string WhatToClick { get; init; }
    public required string HowToFindIt { get; init; }
}

public sealed class StatCard
{
    public required string Label { get; init; }
    public required string Value { get; init; }
    public required string Caption { get; init; }
}

public sealed class MetricBar
{
    public required string Label { get; init; }
    public required double Value { get; init; }
    public required string DisplayValue { get; init; }
}

public sealed class DemoTimelineStage
{
    public required string Title { get; init; }
    public required string Summary { get; init; }
    public required string StatusLabel { get; init; }
    public required string PageKey { get; init; }
    public required string ActionLabel { get; init; }
    public required Brush AccentBrush { get; init; }
}

public sealed class ArchitectureStage
{
    public required string StepLabel { get; init; }
    public required string Title { get; init; }
    public required string Summary { get; init; }
    public required Brush AccentBrush { get; init; }
}

public sealed class RunSummary
{
    public required string Name { get; init; }
    public required string Family { get; init; }
    public required string HeadlineMetric { get; init; }
    public required string SupportMetric { get; init; }
    public required string Note { get; init; }
    public required Brush AccentBrush { get; init; }
}

public sealed class ArtifactSummary
{
    public required string Title { get; init; }
    public required string Status { get; init; }
    public required string Summary { get; init; }
}

public sealed class PredictionSummary
{
    public required string PairLabel { get; init; }
    public required string Score { get; init; }
    public required string Confidence { get; init; }
    public required string Rationale { get; init; }
    public required string RiskNote { get; init; }
}

public sealed class ChoiceItem
{
    public required string Key { get; init; }
    public required string Label { get; init; }
    public required string Caption { get; init; }
}

public static class DemoPalette
{
    public static SolidColorBrush Aqua { get; } = new(ColorHelper.FromArgb(255, 18, 191, 198));
    public static SolidColorBrush Blue { get; } = new(ColorHelper.FromArgb(255, 29, 78, 216));
    public static SolidColorBrush Gold { get; } = new(ColorHelper.FromArgb(255, 246, 196, 83));
    public static SolidColorBrush Coral { get; } = new(ColorHelper.FromArgb(255, 251, 113, 133));
    public static SolidColorBrush Slate { get; } = new(ColorHelper.FromArgb(255, 84, 104, 129));
}
