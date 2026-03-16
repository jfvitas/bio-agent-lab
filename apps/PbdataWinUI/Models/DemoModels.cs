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
    public string? Path { get; init; }
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

public sealed class WorkspaceStageInfo
{
    public required string StageKey { get; init; }
    public required string Status { get; init; }
    public required string Note { get; init; }
}

public sealed class WorkspaceModelRunInfo
{
    public required string RunName { get; init; }
    public required string Family { get; init; }
    public required string HeadlineMetric { get; init; }
    public required string SupportMetric { get; init; }
    public required string Note { get; init; }
    public double SortMetric { get; init; }
    public IReadOnlyList<double> TrainingHistory { get; init; } = Array.Empty<double>();
    public IReadOnlyList<double> ValidationHistory { get; init; } = Array.Empty<double>();
}

public sealed class WorkspacePredictionInfo
{
    public required string PairLabel { get; init; }
    public required string Score { get; init; }
    public required string Confidence { get; init; }
    public required string Rationale { get; init; }
    public required string RiskNote { get; init; }
}

public sealed class WorkspaceGraphPackageInfo
{
    public required bool IsPresent { get; init; }
    public required string ManifestPath { get; init; }
    public required string GraphLevel { get; init; }
    public required string Scope { get; init; }
    public required string Selection { get; init; }
    public required string GeneratedAt { get; init; }
    public required IReadOnlyList<string> ExportFormats { get; init; }
    public int SelectedCount { get; init; }
    public int ProcessedCount { get; init; }
    public int SkippedCount { get; init; }
    public int GraphCount { get; init; }
}

public sealed class WorkspaceSnapshot
{
    public required string RootPath { get; init; }
    public required string Headline { get; init; }
    public required string Disclaimer { get; init; }
    public required string Summary { get; init; }
    public required string Readiness { get; init; }
    public required string SourceSummary { get; init; }
    public required string SplitSummary { get; init; }
    public required string BootstrapNarrative { get; init; }
    public required string RefreshPlanNarrative { get; init; }
    public required string GraphPackageNarrative { get; init; }
    public required string EnvironmentGuidance { get; init; }
    public required string EnvironmentFixCommands { get; init; }
    public required string ModelHeadline { get; init; }
    public required string ModelPitch { get; init; }
    public required string RunSummary { get; init; }
    public required string SelectedRunName { get; init; }
    public required string InferenceNarrative { get; init; }
    public required IReadOnlyList<StatCard> BootstrapStats { get; init; }
    public required IReadOnlyList<StatCard> RefreshPlanStats { get; init; }
    public required IReadOnlyList<StatCard> GraphPackageStats { get; init; }
    public required IReadOnlyList<StatCard> DatasetStats { get; init; }
    public required IReadOnlyList<StatCard> EnvironmentStats { get; init; }
    public required IReadOnlyList<StatCard> TrainingStats { get; init; }
    public required IReadOnlyList<MetricBar> MetricBars { get; init; }
    public required IReadOnlyList<ArchitectureStage> ArchitectureStages { get; init; }
    public required IReadOnlyList<WorkspaceModelRunInfo> ModelRuns { get; init; }
    public required IReadOnlyList<ArtifactSummary> Artifacts { get; init; }
    public required IReadOnlyList<WorkspacePredictionInfo> Predictions { get; init; }
    public required IReadOnlyList<string> ActivityLog { get; init; }
    public required IReadOnlyList<WorkspaceStageInfo> StageStatuses { get; init; }
    public required WorkspaceGraphPackageInfo GraphPackage { get; init; }
    public IReadOnlyList<double> TrainingCurveValues { get; init; } = Array.Empty<double>();
    public IReadOnlyList<double> ValidationCurveValues { get; init; } = Array.Empty<double>();
}

public static class DemoPalette
{
    public static SolidColorBrush Aqua { get; } = new(ColorHelper.FromArgb(255, 18, 191, 198));
    public static SolidColorBrush Blue { get; } = new(ColorHelper.FromArgb(255, 29, 78, 216));
    public static SolidColorBrush Gold { get; } = new(ColorHelper.FromArgb(255, 246, 196, 83));
    public static SolidColorBrush Coral { get; } = new(ColorHelper.FromArgb(255, 251, 113, 133));
    public static SolidColorBrush Slate { get; } = new(ColorHelper.FromArgb(255, 84, 104, 129));
}
