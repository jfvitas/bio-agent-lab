using System.Text.Json;

namespace PbdataWinUI.Services;

public sealed class WorkspaceDataService
{
    private sealed record BootstrapStoreSummary(
        bool IsPresent,
        string ManifestPath,
        string DatabasePath,
        int RecordCount,
        int SourceInputCount,
        string GeneratedAt);

    private sealed record RefreshPlanSummary(
        bool IsPresent,
        string ManifestPath,
        string SelectedSource,
        int RecordCount,
        string GeneratedAt);

    private sealed record GraphPackageSummary(
        bool IsPresent,
        string ManifestPath,
        string GraphLevel,
        string Scope,
        string Selection,
        string GeneratedAt,
        IReadOnlyList<string> ExportFormats,
        int SelectedCount,
        int ProcessedCount,
        int SkippedCount,
        int GraphCount);

    private sealed record DatasetGraphSummary(
        bool IsPresent,
        string GraphConfigPath,
        string DiversityReportPath,
        string DatasetName,
        string GraphLevel,
        string GraphScope,
        string GraphSelection,
        IReadOnlyList<string> ExportFormats,
        int GraphCoveredRows,
        double GraphCoveredFraction);

    private sealed record ModelRecommendationSummary(
        bool IsPresent,
        string JsonPath,
        string MarkdownPath,
        string Status,
        string Summary,
        string NextAction,
        string Label,
        string Family,
        string Modality,
        string WhyItFits,
        string StarterConfigPath,
        string DatasetSource,
        double GraphCoveredFraction,
        int GraphCoveredRows);

    private sealed record RuntimeEnvironmentSummary(
        bool UsesLocalVenv,
        string PythonPath,
        bool HasSklearn,
        bool HasTorch,
        bool HasTorchGeometric,
        bool HasXgboost,
        bool HasPyarrow,
        bool HasFastparquet,
        bool HasEsm,
        string InstalledBackendsLabel);

    public string DetectWorkspaceRoot()
    {
        foreach (var candidate in EnumerateRootCandidates())
        {
            if (LooksLikeWorkspaceRoot(candidate))
            {
                return candidate;
            }
        }
        return Directory.GetCurrentDirectory();
    }

    public WorkspaceSnapshot LoadSnapshot(string workspaceRoot)
    {
        var root = Path.GetFullPath(workspaceRoot);
        var readinessPath = Path.Combine(root, "artifacts", "reports", "demo_readiness.json");
        var releasePath = Path.Combine(root, "release_readiness_report.json");
        var scorecardPath = Path.Combine(root, "custom_training_scorecard.json");
        var stageDir = Path.Combine(root, "data", "catalog", "stage_state");
        var predictionDir = Path.Combine(root, "data", "prediction");
        var modelRunsDir = Path.Combine(root, "data", "models", "model_studio", "runs");
        var bootstrapCatalogDir = Path.Combine(root, "metadata", "bootstrap_catalog");
        var reportsDir = Path.Combine(root, "data", "reports");
        var bootstrapStoreManifestPath = Path.Combine(bootstrapCatalogDir, "bootstrap_store_manifest.json");
        var bootstrapStoreDatabasePath = Path.Combine(bootstrapCatalogDir, "bootstrap_catalog.sqlite");
        var refreshPlanManifestPath = Path.Combine(bootstrapCatalogDir, "selected_pdb_refresh_manifest.json");
        var graphPackage = ReadLatestGraphPackageSummary(Path.Combine(root, "graphs"));
        var datasetGraph = ReadLatestDatasetGraphSummary(Path.Combine(root, "datasets"));
        var modelRecommendation = ReadModelRecommendationSummary(
            Path.Combine(reportsDir, "model_studio_recommendation.json"),
            Path.Combine(reportsDir, "model_studio_recommendation.md"));
        var runtimeEnvironment = ReadRuntimeEnvironmentSummary(root);

        var readiness = ReadObject(readinessPath);
        var statusSnapshot = ReadObjectFromObject(readiness, "status_snapshot");
        var scorecard = ReadObject(scorecardPath);
        var release = ReadObject(releasePath);
        var bootstrapStore = ReadBootstrapStoreSummary(bootstrapStoreManifestPath, bootstrapStoreDatabasePath);
        var refreshPlan = ReadRefreshPlanSummary(refreshPlanManifestPath);

        var extractedCount = GetInt(statusSnapshot, "extracted_entry_count");
        var structureCount = GetInt(statusSnapshot, "structure_file_count");
        var trainingExampleCount = GetInt(statusSnapshot, "training_example_count");
        var siteFeatureRuns = GetInt(statusSnapshot, "site_feature_runs");
        var modelReadyCount = GetInt(scorecard, "selected_count");
        var candidatePoolCount = GetInt(scorecard, "candidate_pool_count");
        var readinessLabel = HumanizeReadinessLabel(ReadString(readiness, "readiness", "workspace detected"));
        var warnings = ReadStringList(readiness, "warnings");
        var readinessSummary = BuildPresentationSummary(
            ReadString(readiness, "summary", "Workspace state is available for inspection."),
            warnings);
        var sourceSummary = BuildSourceSummary(statusSnapshot, readiness, warnings);
        var splitSummary = BuildSplitSummary(scorecard, release, warnings);
        var bootstrapNarrative = BuildBootstrapNarrative(bootstrapStore);
        var refreshPlanNarrative = BuildRefreshPlanNarrative(refreshPlan);
        var graphPackageNarrative = BuildGraphPackageNarrative(graphPackage);
        var environmentGuidance = BuildEnvironmentGuidance(runtimeEnvironment);
        var environmentFixCommands = BuildEnvironmentFixCommands(runtimeEnvironment);

        var trainingStats = BuildTrainingStats(scorecard, release, statusSnapshot, datasetGraph, modelRecommendation, runtimeEnvironment);
        var environmentStats = BuildEnvironmentStats(root, runtimeEnvironment);
        var metricBars = BuildMetricBars(scorecard, release, statusSnapshot);
        var architectureStages = BuildArchitectureStages(statusSnapshot);
        var modelRuns = BuildModelRuns(modelRunsDir);
        var artifacts = BuildArtifacts(
            root,
            readinessPath,
            releasePath,
            scorecardPath,
            bootstrapStoreManifestPath,
            bootstrapStoreDatabasePath,
            refreshPlanManifestPath,
            modelRecommendation.JsonPath,
            modelRecommendation.MarkdownPath,
            modelRecommendation.StarterConfigPath,
            datasetGraph.GraphConfigPath,
            datasetGraph.DiversityReportPath);
        var predictions = BuildPredictions(predictionDir);
        var activityLog = BuildActivityLog(stageDir);
        var stages = BuildStageStatuses(
            stageDir,
            readinessSummary,
            bootstrapStore,
            refreshPlan,
            modelRecommendation,
            predictions.Count > 0,
            modelRuns.Count > 0);

        var selectedRun = modelRuns.FirstOrDefault();
        var selectedRunName = selectedRun?.RunName ?? "No saved model run detected";
        var runSummary = selectedRun?.Note
            ?? "No model run metrics are available yet. Train or import a model run to populate this view.";
        var modelHeadline = selectedRun is { } bestRun
            ? $"{HumanizeFamily(bestRun.Family)} is the strongest detected saved run in this workspace."
            : BuildDefaultModelHeadline(datasetGraph, modelRecommendation);
        var modelPitch = modelRuns.Count > 0
            ? $"Detected {modelRuns.Count} saved run(s) under data/models/model_studio/runs with metrics and manifests."
            : BuildDefaultModelPitch(datasetGraph, modelRecommendation);
        var inferenceNarrative = predictions.Count > 0
            ? "Inference surfaces are now reading from saved prediction manifests in the current workspace."
            : "No prediction manifests were found yet; when inference runs are generated, ranked outputs will appear here automatically.";

        return new WorkspaceSnapshot
        {
            RootPath = root,
            Headline = "ProteoSphere turns broad structural evidence into balanced ML-ready datasets, workflow-aware model selection, and explainable prediction outputs.",
            Disclaimer = warnings.Contains("demo_mode_simulated_outputs", StringComparer.OrdinalIgnoreCase)
                ? "A guided, local-first platform view for showing how ProteoSphere assembles balanced datasets, selects model strategies, and reviews explainable predictions."
                : "A live platform view of the current workspace state, including datasets, model runs, outputs, and workflow status.",
            Summary = readinessSummary,
            Readiness = readinessLabel,
            SourceSummary = sourceSummary,
            SplitSummary = splitSummary,
            BootstrapNarrative = bootstrapNarrative,
            RefreshPlanNarrative = refreshPlanNarrative,
            GraphPackageNarrative = graphPackageNarrative,
            EnvironmentGuidance = environmentGuidance,
            EnvironmentFixCommands = environmentFixCommands,
            ModelHeadline = modelHeadline,
            ModelPitch = modelPitch,
            RunSummary = runSummary,
            SelectedRunName = selectedRunName,
            InferenceNarrative = inferenceNarrative,
            BootstrapStats = BuildBootstrapStats(bootstrapStore),
            RefreshPlanStats = BuildRefreshPlanStats(refreshPlan),
            GraphPackageStats = BuildGraphPackageStats(graphPackage),
            DatasetStats = new[]
            {
                new StatCard { Label = "Extracted entries", Value = $"{extractedCount:N0}", Caption = "Entries materialized into extracted tables under data/extracted." },
                new StatCard { Label = "Structure files", Value = $"{structureCount:N0}", Caption = "Local structure files available for graph and feature stages." },
                new StatCard { Label = "Training examples", Value = $"{trainingExampleCount:N0}", Caption = "Current pair-level training examples available for downstream modeling." },
                new StatCard { Label = "Model-ready pairs", Value = $"{modelReadyCount:N0}", Caption = candidatePoolCount > 0 ? $"Selected from a candidate pool of {candidatePoolCount:N0} pairs." : "Selection scorecard not found or not built yet." },
            },
            EnvironmentStats = environmentStats,
            TrainingStats = trainingStats,
            MetricBars = metricBars,
            ArchitectureStages = architectureStages,
            ModelRuns = modelRuns,
            Artifacts = artifacts,
            Predictions = predictions,
            ActivityLog = activityLog,
            StageStatuses = stages,
            GraphPackage = new WorkspaceGraphPackageInfo
            {
                IsPresent = graphPackage.IsPresent,
                ManifestPath = graphPackage.ManifestPath,
                GraphLevel = graphPackage.GraphLevel,
                Scope = graphPackage.Scope,
                Selection = graphPackage.Selection,
                GeneratedAt = graphPackage.GeneratedAt,
                ExportFormats = graphPackage.ExportFormats,
                SelectedCount = graphPackage.SelectedCount,
                ProcessedCount = graphPackage.ProcessedCount,
                SkippedCount = graphPackage.SkippedCount,
                GraphCount = graphPackage.GraphCount,
            },
            TrainingCurveValues = selectedRun?.TrainingHistory ?? Array.Empty<double>(),
            ValidationCurveValues = selectedRun?.ValidationHistory ?? Array.Empty<double>(),
        };
    }

    private static IEnumerable<string> EnumerateRootCandidates()
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var seed in new[]
                 {
                     Directory.GetCurrentDirectory(),
                     AppContext.BaseDirectory,
                 })
        {
            var current = Path.GetFullPath(seed);
            while (!string.IsNullOrWhiteSpace(current))
            {
                if (seen.Add(current))
                {
                    yield return current;
                }
                var parent = Directory.GetParent(current);
                if (parent is null)
                {
                    break;
                }
                current = parent.FullName;
            }
        }
    }

    private static bool LooksLikeWorkspaceRoot(string candidate)
    {
        return File.Exists(Path.Combine(candidate, "AGENTS.md"))
            && Directory.Exists(Path.Combine(candidate, "apps", "PbdataWinUI"));
    }

    private static Dictionary<string, object?> ReadObject(string path)
    {
        if (!File.Exists(path))
        {
            return new Dictionary<string, object?>();
        }
        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(path));
            return document.RootElement.ValueKind == JsonValueKind.Object
                ? ConvertObject(document.RootElement)
                : new Dictionary<string, object?>();
        }
        catch
        {
            return new Dictionary<string, object?>();
        }
    }

    private static Dictionary<string, object?> ReadObjectFromObject(
        Dictionary<string, object?> source,
        string key)
    {
        return source.TryGetValue(key, out var value) && value is Dictionary<string, object?> dict
            ? dict
            : new Dictionary<string, object?>();
    }

    private static Dictionary<string, object?> ConvertObject(JsonElement element)
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in element.EnumerateObject())
        {
            result[property.Name] = ConvertValue(property.Value);
        }
        return result;
    }

    private static object? ConvertValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Object => ConvertObject(element),
            JsonValueKind.Array => element.EnumerateArray().Select(ConvertValue).ToList(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var integer) ? integer : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    private static int GetInt(Dictionary<string, object?> source, string key)
    {
        if (!source.TryGetValue(key, out var value) || value is null)
        {
            return 0;
        }
        return value switch
        {
            int intValue => intValue,
            long longValue => (int)longValue,
            double doubleValue => (int)Math.Round(doubleValue),
            string text when int.TryParse(text.Replace(",", "").Trim(), out var parsed) => parsed,
            _ => 0,
        };
    }

    private static int GetInt(Dictionary<string, object?> source, params string[] path)
    {
        object? current = source;
        foreach (var part in path)
        {
            if (current is Dictionary<string, object?> dict && dict.TryGetValue(part, out var next))
            {
                current = next;
            }
            else
            {
                return 0;
            }
        }
        return current switch
        {
            int intValue => intValue,
            long longValue => (int)longValue,
            double doubleValue => (int)Math.Round(doubleValue),
            string text when int.TryParse(text.Replace(",", "").Trim(), out var parsed) => parsed,
            _ => 0,
        };
    }

    private static double GetDouble(Dictionary<string, object?> source, params string[] path)
    {
        object? current = source;
        foreach (var part in path)
        {
            if (current is Dictionary<string, object?> dict && dict.TryGetValue(part, out var next))
            {
                current = next;
            }
            else
            {
                return 0.0;
            }
        }
        return current switch
        {
            double doubleValue => doubleValue,
            int intValue => intValue,
            long longValue => longValue,
            string text when double.TryParse(text, out var parsed) => parsed,
            _ => 0.0,
        };
    }

    private static string ReadString(Dictionary<string, object?> source, string key, string fallback = "")
    {
        return source.TryGetValue(key, out var value) && value is not null
            ? Convert.ToString(value) ?? fallback
            : fallback;
    }

    private static List<string> ReadStringList(Dictionary<string, object?> source, string key)
    {
        if (!source.TryGetValue(key, out var value) || value is not IEnumerable<object?> values)
        {
            return new List<string>();
        }
        return values
            .Select(item => Convert.ToString(item) ?? string.Empty)
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .ToList();
    }

    private static string BuildSourceSummary(
        Dictionary<string, object?> statusSnapshot,
        Dictionary<string, object?> readiness,
        IReadOnlyCollection<string> warnings)
    {
        var raw = GetInt(statusSnapshot, "raw_rcsb_count");
        var extracted = GetInt(statusSnapshot, "extracted_entry_count");
        var structures = GetInt(statusSnapshot, "structure_file_count");
        var latestStage = ReadString(statusSnapshot, "latest_stage_name", "unknown");
        var latestStatus = ReadString(statusSnapshot, "latest_stage_status", "unknown");
        var caveat = warnings.Contains("demo_mode_simulated_outputs", StringComparer.OrdinalIgnoreCase)
            ? " Curated walkthrough state is available for parts of the workflow that have not been rerun locally yet."
            : string.Empty;
        return $"Workspace root currently surfaces {raw:N0} raw records, {extracted:N0} extracted entries, and {structures:N0} structure files. Latest stage state: {latestStage} ({latestStatus}).{caveat}";
    }

    private static string BuildSplitSummary(
        Dictionary<string, object?> scorecard,
        Dictionary<string, object?> release,
        IReadOnlyCollection<string> warnings)
    {
        var summary = ReadString(scorecard, "selection_summary", "");
        var interpretation = ReadString(scorecard, "interpretation", "");
        var releaseStatus = ReadString(release, "status", "");
        var warningSuffix = warnings.Contains("demo_mode_simulated_outputs", StringComparer.OrdinalIgnoreCase)
            ? " This view can also highlight curated walkthrough outputs when a stage has not been rerun yet."
            : string.Empty;

        if (!string.IsNullOrWhiteSpace(summary) || !string.IsNullOrWhiteSpace(interpretation))
        {
            return $"{summary} {interpretation} Release status: {releaseStatus}.{warningSuffix}".Trim();
        }

        return $"No split scorecard was detected. Release status: {releaseStatus}.{warningSuffix}".Trim();
    }

    private static IReadOnlyList<StatCard> BuildTrainingStats(
        Dictionary<string, object?> scorecard,
        Dictionary<string, object?> release,
        Dictionary<string, object?> statusSnapshot,
        DatasetGraphSummary datasetGraph,
        ModelRecommendationSummary recommendation,
        RuntimeEnvironmentSummary runtimeEnvironment)
    {
        var releaseStatus = ReadString(release, "status", "unknown");
        var runtimeMode = DescribeRecommendationRuntime(recommendation, runtimeEnvironment);
        return new[]
        {
            new StatCard { Label = "Selected examples", Value = $"{GetInt(scorecard, "selected_count"):N0}", Caption = "Examples selected into the current custom training set." },
            new StatCard { Label = "Candidate pool", Value = $"{GetInt(scorecard, "candidate_pool_count"):N0}", Caption = "Candidate model-ready pairs considered during selection." },
            new StatCard
            {
                Label = "Graph coverage",
                Value = datasetGraph.IsPresent ? $"{datasetGraph.GraphCoveredFraction:P0}" : "n/a",
                Caption = datasetGraph.IsPresent
                    ? $"{datasetGraph.GraphCoveredRows:N0} rows align to the latest engineered dataset graph package."
                    : "No recent engineered dataset graph coverage report was found yet."
            },
            new StatCard { Label = "Runtime backends", Value = runtimeEnvironment.InstalledBackendsLabel, Caption = runtimeEnvironment.UsesLocalVenv ? $"Using local environment at {runtimeEnvironment.PythonPath}." : "Using system Python fallback; local package availability may differ." },
            new StatCard { Label = "Recommendation runtime", Value = runtimeMode, Caption = recommendation.IsPresent ? BuildRuntimeCaption(recommendation, runtimeEnvironment, runtimeMode) : "Export a model recommendation to see whether the current machine can run it natively." },
            new StatCard { Label = "Release check", Value = releaseStatus, Caption = "Current root-level release readiness report status." },
        };
    }

    private static string BuildDefaultModelHeadline(
        DatasetGraphSummary datasetGraph,
        ModelRecommendationSummary recommendation)
    {
        if (recommendation.IsPresent && !string.IsNullOrWhiteSpace(recommendation.Label))
        {
            return $"{recommendation.Label} is the current workspace-backed recommended starting model path.";
        }

        if (!datasetGraph.IsPresent)
        {
            return "Model Studio is ready, but it is still waiting on an engineered dataset with graph coverage metadata.";
        }

        if (datasetGraph.GraphCoveredFraction >= 0.8)
        {
            return "The latest engineered dataset is graph-rich enough to support a graph-heavy or hybrid model path.";
        }

        if (datasetGraph.GraphCoveredFraction >= 0.4)
        {
            return "The latest engineered dataset supports a balanced hybrid path, but graph coverage is still uneven.";
        }

        return "The latest engineered dataset is graph-light, so a stronger tabular baseline is the safer starting point.";
    }

    private static string BuildDefaultModelPitch(
        DatasetGraphSummary datasetGraph,
        ModelRecommendationSummary recommendation)
    {
        if (recommendation.IsPresent)
        {
            var graphCoverageText = recommendation.GraphCoveredRows > 0
                ? $" Graph coverage is {recommendation.GraphCoveredFraction:P0} across {recommendation.GraphCoveredRows:N0} rows."
                : string.Empty;
            var starterText = !string.IsNullOrWhiteSpace(recommendation.StarterConfigPath)
                ? $" Starter config: {recommendation.StarterConfigPath}."
                : string.Empty;
            return $"{recommendation.Summary} Why it fits: {recommendation.WhyItFits} Next action: {recommendation.NextAction}.{graphCoverageText}{starterText}";
        }

        if (!datasetGraph.IsPresent)
        {
            return "No recent engineered dataset graph_config.json was found, so Model Studio cannot yet use live graph coverage to frame the next model recommendation.";
        }

        var formats = datasetGraph.ExportFormats.Count > 0
            ? string.Join(", ", datasetGraph.ExportFormats)
            : "no exports recorded";
        return $"Latest engineered dataset: {datasetGraph.DatasetName}. Graph coverage is {datasetGraph.GraphCoveredFraction:P0} across {datasetGraph.GraphCoveredRows:N0} rows using {datasetGraph.GraphLevel}/{datasetGraph.GraphScope} graphs. Export bundle: {formats}.";
    }

    private static IReadOnlyList<MetricBar> BuildMetricBars(
        Dictionary<string, object?> scorecard,
        Dictionary<string, object?> release,
        Dictionary<string, object?> statusSnapshot)
    {
        var quality = GetDouble(scorecard, "quality", "mean_quality_score");
        var diversity = GetInt(scorecard, "diversity", "selected_receptor_clusters");
        var selected = Math.Max(GetInt(scorecard, "selected_count"), 1);
        var candidatePool = Math.Max(GetInt(scorecard, "candidate_pool_count"), 1);
        var featureRuns = GetInt(statusSnapshot, "site_feature_runs");
        var advancedReady = ReadString(release, "status", "").Equals("ready", StringComparison.OrdinalIgnoreCase) ? 0.92 : 0.58;

        return new[]
        {
            new MetricBar { Label = "Selection quality", Value = Clamp01(quality), DisplayValue = quality > 0 ? quality.ToString("0.000") : "n/a" },
            new MetricBar { Label = "Pool coverage", Value = Clamp01(selected / candidatePool), DisplayValue = $"{selected:N0}/{candidatePool:N0}" },
            new MetricBar { Label = "Cluster diversity", Value = Clamp01(diversity / 500.0), DisplayValue = $"{diversity:N0} clusters" },
            new MetricBar { Label = "Workflow maturity", Value = Clamp01((featureRuns / 6.0 + advancedReady) / 2.0), DisplayValue = ReadString(release, "status", "unknown") },
        };
    }

    private static string BuildBootstrapNarrative(BootstrapStoreSummary summary)
    {
        if (!summary.IsPresent)
        {
            return "No local bootstrap store is present yet. Build it once to cache compact, per-PDB facts locally before training-set design and targeted refresh.";
        }

        return $"The local bootstrap store indexes {summary.RecordCount:N0} PDB records in a fast sqlite layer backed by {summary.SourceInputCount:N0} workspace source tables. This is the operational starting point for curation and split design.";
    }

    private static string BuildRefreshPlanNarrative(RefreshPlanSummary summary)
    {
        if (!summary.IsPresent)
        {
            return "No selected-PDB refresh plan has been generated yet. Once the active training set is chosen, ProteoSphere can plan a narrow recheck of just those PDB IDs instead of reprocessing the full corpus.";
        }

        var sourceLabel = string.IsNullOrWhiteSpace(summary.SelectedSource)
            ? "the active training selection"
            : Path.GetFileName(summary.SelectedSource);
        return $"The current targeted refresh plan tracks {summary.RecordCount:N0} selected PDB IDs sourced from {sourceLabel}, so upstream rechecks stay focused on the dataset that actually matters for training and review.";
    }

    private static string BuildGraphPackageNarrative(GraphPackageSummary summary)
    {
        if (!summary.IsPresent)
        {
            return "No structural graph package has been materialized yet. Once graphs are built, the latest manifest will summarize scope, selected coverage, reuse, and export formats.";
        }

        var formats = summary.ExportFormats.Count > 0
            ? string.Join(", ", summary.ExportFormats)
            : "no exports recorded";
        return $"The latest structural graph package covers {summary.GraphCount:N0} PDB graphs using {summary.GraphLevel} / {summary.Scope} design. Selection source: {summary.Selection}. Export bundle: {formats}.";
    }

    private static IReadOnlyList<StatCard> BuildBootstrapStats(BootstrapStoreSummary summary)
    {
        return new[]
        {
            new StatCard
            {
                Label = "Bootstrap store",
                Value = summary.IsPresent ? "Ready" : "Missing",
                Caption = summary.IsPresent ? "Indexed local PDB facts are available for fast planning." : "Run materialize-bootstrap-store to build the local-first cache."
            },
            new StatCard
            {
                Label = "Indexed PDBs",
                Value = summary.IsPresent ? $"{summary.RecordCount:N0}" : "0",
                Caption = "Compact bootstrap coverage available before targeted refresh or modeling."
            },
            new StatCard
            {
                Label = "Source tables",
                Value = summary.IsPresent ? $"{summary.SourceInputCount:N0}" : "0",
                Caption = "Workspace source inputs tracked in the bootstrap manifest."
            },
            new StatCard
            {
                Label = "Backing store",
                Value = summary.IsPresent && File.Exists(summary.DatabasePath) ? "sqlite" : "not built",
                Caption = summary.IsPresent ? summary.DatabasePath : "The indexed sqlite file will be created under metadata/bootstrap_catalog."
            },
        };
    }

    private static IReadOnlyList<StatCard> BuildRefreshPlanStats(RefreshPlanSummary summary)
    {
        var selectedSource = string.IsNullOrWhiteSpace(summary.SelectedSource)
            ? "Not planned"
            : Path.GetFileName(summary.SelectedSource);

        return new[]
        {
            new StatCard
            {
                Label = "Refresh plan",
                Value = summary.IsPresent ? "Ready" : "Pending",
                Caption = summary.IsPresent ? "Selected-PDB refresh is scoped and ready to run when needed." : "Generate a plan after the active training set is assembled."
            },
            new StatCard
            {
                Label = "Selected PDBs",
                Value = summary.IsPresent ? $"{summary.RecordCount:N0}" : "0",
                Caption = "Only these structures need optional update checks after curation."
            },
            new StatCard
            {
                Label = "Refresh source",
                Value = selectedSource,
                Caption = "The current training-set source used to define the refresh scope."
            },
            new StatCard
            {
                Label = "Last planned",
                Value = summary.IsPresent ? FormatTimestamp(summary.GeneratedAt) : "Not yet",
                Caption = summary.IsPresent ? summary.ManifestPath : "The refresh manifest will be written under metadata/bootstrap_catalog."
            },
        };
    }

    private static IReadOnlyList<StatCard> BuildGraphPackageStats(GraphPackageSummary summary)
    {
        return new[]
        {
            new StatCard
            {
                Label = "Graph package",
                Value = summary.IsPresent ? "Ready" : "Missing",
                Caption = summary.IsPresent ? "Latest graph manifest is available from the workspace." : "Build structural graphs to materialize a graph package and manifest."
            },
            new StatCard
            {
                Label = "Graph entries",
                Value = summary.IsPresent ? $"{summary.GraphCount:N0}" : "0",
                Caption = "PDB graphs listed in the latest graph manifest."
            },
            new StatCard
            {
                Label = "Built / reused",
                Value = summary.IsPresent ? $"{summary.ProcessedCount:N0} / {summary.SkippedCount:N0}" : "0 / 0",
                Caption = "Freshly processed graphs versus cache hits in the latest package run."
            },
            new StatCard
            {
                Label = "Package mode",
                Value = summary.IsPresent ? $"{summary.GraphLevel} / {summary.Scope}" : "Not built",
                Caption = summary.IsPresent ? FormatTimestamp(summary.GeneratedAt) : "The latest graph package timestamp will appear here."
            },
        };
    }

    private static IReadOnlyList<ArchitectureStage> BuildArchitectureStages(Dictionary<string, object?> statusSnapshot)
    {
        return new[]
        {
            new ArchitectureStage { StepLabel = "1", Title = "Ingest and normalize", Summary = $"{GetInt(statusSnapshot, "raw_rcsb_count"):N0} raw records and {GetInt(statusSnapshot, "processed_rcsb_valid_count"):N0} validated processed records are currently visible.", AccentBrush = DemoPalette.Aqua },
            new ArchitectureStage { StepLabel = "2", Title = "Extract and curate", Summary = $"{GetInt(statusSnapshot, "extracted_entry_count"):N0} extracted entries and {GetInt(statusSnapshot, "structure_file_count"):N0} local structures are available for downstream stages.", AccentBrush = DemoPalette.Blue },
            new ArchitectureStage { StepLabel = "3", Title = "Graph and feature layers", Summary = $"{(GetInt(statusSnapshot, "graph_node_export_present") > 0 || ReadString(statusSnapshot, "graph_node_export_present").Equals("True", StringComparison.OrdinalIgnoreCase) ? "Graph exports are present." : "Graph exports are still missing.")} Feature manifests: {(ReadString(statusSnapshot, "feature_manifest_present", "").Equals("True", StringComparison.OrdinalIgnoreCase) ? "present" : "missing")}.", AccentBrush = DemoPalette.Gold },
            new ArchitectureStage { StepLabel = "4", Title = "Training and release", Summary = $"{GetInt(statusSnapshot, "training_example_count"):N0} training examples and release snapshot present: {ReadString(statusSnapshot, "release_snapshot_present", "false")}.", AccentBrush = DemoPalette.Coral },
        };
    }

    private static IReadOnlyList<WorkspaceModelRunInfo> BuildModelRuns(string runsDir)
    {
        if (!Directory.Exists(runsDir))
        {
            return Array.Empty<WorkspaceModelRunInfo>();
        }

        var runs = new List<WorkspaceModelRunInfo>();
        foreach (var runDir in Directory.GetDirectories(runsDir))
        {
            var manifest = ReadObject(Path.Combine(runDir, "run_manifest.json"));
            var metrics = ReadObject(Path.Combine(runDir, "metrics.json"));
            var runName = ReadString(manifest, "run_name", Path.GetFileName(runDir));
            var family = ReadString(manifest, "family", "unknown");
            var backendPlan = ReadObjectFromObject(manifest, "backend_plan");
            var backend = ReadString(manifest, "backend", "saved run");
            var executionStrategy = ReadString(manifest, "execution_strategy", "auto");
            var runtimeAdjustment = ReadString(manifest, "runtime_adjustment", string.Empty);
            var requestedFamily = ReadString(backendPlan, "requested_family", family);
            var executionFamily = ReadString(backendPlan, "execution_family", family);
            var testRmse = GetDouble(metrics, "test", "rmse");
            var testMae = GetDouble(metrics, "test", "mae");
            var history = ReadArray(Path.Combine(runDir, "history.json"));
            var trainingHistory = history
                .Select(item => GetDouble(item, "train_metric"))
                .Where(value => value > 0)
                .ToList();
            var validationHistory = history
                .Select(item => GetDouble(item, "val_metric"))
                .Where(value => value > 0)
                .ToList();
            runs.Add(new WorkspaceModelRunInfo
            {
                RunName = runName,
                Family = family,
                HeadlineMetric = testRmse > 0 ? $"Test RMSE {testRmse:0.000}" : "Metrics pending",
                SupportMetric = testMae > 0
                    ? $"Test MAE {testMae:0.000}"
                    : BuildRunSupportMetric(backend, requestedFamily, executionFamily, executionStrategy),
                Note = BuildRunNote(manifest, runtimeAdjustment),
                SortMetric = testRmse > 0 ? testRmse : double.MaxValue,
                TrainingHistory = trainingHistory,
                ValidationHistory = validationHistory,
            });
        }
        return runs
            .OrderBy(run => run.SortMetric)
            .ThenBy(run => run.RunName, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static string BuildRunSupportMetric(
        string backend,
        string requestedFamily,
        string executionFamily,
        string executionStrategy)
    {
        if (!string.Equals(requestedFamily, executionFamily, StringComparison.OrdinalIgnoreCase))
        {
            return $"{HumanizeExecutionStrategy(executionStrategy)}: {HumanizeFamily(requestedFamily)} -> {HumanizeFamily(executionFamily)}";
        }

        return $"Backend {backend}";
    }

    private static string BuildRunNote(
        Dictionary<string, object?> manifest,
        string runtimeAdjustment)
    {
        if (!string.IsNullOrWhiteSpace(runtimeAdjustment))
        {
            return runtimeAdjustment;
        }

        var warning = ReadStringList(manifest, "warnings").FirstOrDefault();
        if (!string.IsNullOrWhiteSpace(warning))
        {
            return warning;
        }

        return $"Runtime target: {ReadString(manifest, "runtime_target", "n/a")} | Mode: {HumanizeExecutionStrategy(ReadString(manifest, "execution_strategy", "auto"))}";
    }

    private static IReadOnlyList<ArtifactSummary> BuildArtifacts(
        string root,
        string readinessPath,
        string releasePath,
        string scorecardPath,
        string bootstrapStoreManifestPath,
        string bootstrapStoreDatabasePath,
        string refreshPlanManifestPath,
        string modelRecommendationJsonPath,
        string modelRecommendationMarkdownPath,
        string modelRecommendationStarterConfigPath,
        string datasetGraphConfigPath,
        string datasetDiversityReportPath)
    {
        var latestGraphManifestPath = FindLatestGraphManifest(Path.Combine(root, "graphs"));
        var artifactCandidates = new[]
        {
            new ArtifactSummary { Title = "Workspace readiness report", Status = File.Exists(readinessPath) ? "present" : "missing", Summary = "Structured readiness snapshot used by the WinUI shell.", Path = readinessPath },
            new ArtifactSummary { Title = "Release readiness report", Status = File.Exists(releasePath) ? "present" : "missing", Summary = "Root-level release readiness JSON for the current workspace.", Path = releasePath },
            new ArtifactSummary { Title = "Training scorecard", Status = File.Exists(scorecardPath) ? "present" : "missing", Summary = "Custom training selection scorecard and interpretation.", Path = scorecardPath },
            new ArtifactSummary { Title = "Bootstrap store manifest", Status = File.Exists(bootstrapStoreManifestPath) ? "present" : "missing", Summary = "Indexed local bootstrap coverage manifest for fast PDB planning.", Path = bootstrapStoreManifestPath },
            new ArtifactSummary { Title = "Bootstrap sqlite store", Status = File.Exists(bootstrapStoreDatabasePath) ? "present" : "missing", Summary = "Persistent sqlite catalog that backs the local-first bootstrap workflow.", Path = bootstrapStoreDatabasePath },
            new ArtifactSummary { Title = "Selected-PDB refresh plan", Status = File.Exists(refreshPlanManifestPath) ? "present" : "missing", Summary = "Targeted refresh manifest scoped to the active training selection.", Path = refreshPlanManifestPath },
            new ArtifactSummary { Title = "Model recommendation report", Status = File.Exists(modelRecommendationJsonPath) ? "present" : "missing", Summary = "Workspace-backed model recommendation export shared by the CLI and WinUI shell.", Path = modelRecommendationJsonPath },
            new ArtifactSummary { Title = "Model recommendation markdown", Status = File.Exists(modelRecommendationMarkdownPath) ? "present" : "missing", Summary = "Readable model recommendation summary with compatibility checks and ranked options.", Path = modelRecommendationMarkdownPath },
            new ArtifactSummary { Title = "Recommended starter config", Status = File.Exists(modelRecommendationStarterConfigPath) ? "present" : "missing", Summary = "Starter configuration emitted for the current top-ranked model recommendation.", Path = modelRecommendationStarterConfigPath },
            new ArtifactSummary { Title = "Latest graph package manifest", Status = File.Exists(latestGraphManifestPath) ? "present" : "missing", Summary = "Latest structural graph package summary with selected counts, reuse counts, and scope.", Path = latestGraphManifestPath },
            new ArtifactSummary { Title = "Latest dataset graph config", Status = File.Exists(datasetGraphConfigPath) ? "present" : "missing", Summary = "Latest engineered dataset graph coverage and graph-package alignment config.", Path = datasetGraphConfigPath },
            new ArtifactSummary { Title = "Latest dataset diversity report", Status = File.Exists(datasetDiversityReportPath) ? "present" : "missing", Summary = "Latest engineered dataset diversity and graph coverage summary.", Path = datasetDiversityReportPath },
            new ArtifactSummary { Title = "Demo walkthrough markdown", Status = File.Exists(Path.Combine(root, "artifacts", "reports", "demo_walkthrough.md")) ? "present" : "missing", Summary = "Narrative walkthrough exported from the current workspace.", Path = Path.Combine(root, "artifacts", "reports", "demo_walkthrough.md") },
            new ArtifactSummary { Title = "Saved hybrid run manifest", Status = File.Exists(Path.Combine(root, "data", "models", "model_studio", "runs", "demo_pyg_hybrid_affinity", "run_manifest.json")) ? "present" : "missing", Summary = "Saved run metadata for the strongest detected model-studio experiment.", Path = Path.Combine(root, "data", "models", "model_studio", "runs", "demo_pyg_hybrid_affinity", "run_manifest.json") },
            new ArtifactSummary { Title = "Ligand screening manifest", Status = File.Exists(Path.Combine(root, "data", "prediction", "ligand_screening", "prediction_manifest.json")) ? "present" : "missing", Summary = "Ranked saved-model inference output for the ligand screening workspace.", Path = Path.Combine(root, "data", "prediction", "ligand_screening", "prediction_manifest.json") },
        };
        return artifactCandidates
            .Where(artifact => !string.IsNullOrWhiteSpace(artifact.Path))
            .ToArray();
    }

    private static IReadOnlyList<WorkspacePredictionInfo> BuildPredictions(string predictionDir)
    {
        var predictions = new List<WorkspacePredictionInfo>();
        var ligandPath = Path.Combine(predictionDir, "ligand_screening", "prediction_manifest.json");
        var ligand = ReadObject(ligandPath);
        if (ligand.Count > 0)
        {
            var ranked = ligand.TryGetValue("ranked_target_list", out var rankedValue) && rankedValue is IEnumerable<object?> items
                ? items.OfType<Dictionary<string, object?>>().ToList()
                : new List<Dictionary<string, object?>>();
            foreach (var item in ranked.Take(3))
            {
                var targetId = ReadString(item, "target_id", "unknown target");
                var confidence = GetDouble(item, "confidence_score");
                predictions.Add(new WorkspacePredictionInfo
                {
                    PairLabel = $"Ligand screen target {targetId}",
                    Score = confidence > 0 ? confidence.ToString("0.000") : "n/a",
                    Confidence = confidence >= 0.85 ? "High" : confidence >= 0.7 ? "Medium" : "Low",
                    Rationale = $"Prediction method: {ReadString(ligand, "prediction_method", "unknown")}. Candidate target count: {GetInt(ligand, "candidate_target_count"):N0}.",
                    RiskNote = ReadString(ligand, "notes", "No notes available."),
                });
            }
        }

        var peptidePath = Path.Combine(predictionDir, "peptide_binding", "prediction_manifest.json");
        var peptide = ReadObject(peptidePath);
        if (peptide.Count > 0 && predictions.Count < 3)
        {
            predictions.Add(new WorkspacePredictionInfo
            {
                PairLabel = "Peptide binding workflow",
                Score = ReadString(peptide, "status", "unknown"),
                Confidence = "Workflow",
                Rationale = $"Input type: {ReadString(peptide, "normalized_input_type", "n/a")}. Graph context available: {ReadString(ReadObjectFromObject(peptide, "interface_summary"), "graph_context_available", "false")}.",
                RiskNote = ReadString(peptide, "notes", "No notes available."),
            });
        }

        return predictions;
    }

    private static IReadOnlyList<string> BuildActivityLog(string stageDir)
    {
        if (!Directory.Exists(stageDir))
        {
            return new[] { "No workflow activity has been recorded yet." };
        }

        return Directory.GetFiles(stageDir, "*.json")
            .OrderByDescending(File.GetLastWriteTimeUtc)
            .Take(8)
            .Select(path =>
            {
                var state = ReadObject(path);
                var stage = ReadString(state, "stage", Path.GetFileNameWithoutExtension(path));
                var status = ReadString(state, "status", "unknown");
                var generated = ReadString(state, "generated_at", "");
                var notes = ReadString(state, "notes", "");
                var simulatedSuffix = IsSimulatedPayload(state) ? " [curated snapshot]" : string.Empty;
                return $"[{generated}] {stage}: {status}{simulatedSuffix}{(string.IsNullOrWhiteSpace(notes) ? string.Empty : $" - {notes}")}";
            })
            .ToList();
    }

    private static IReadOnlyList<WorkspaceStageInfo> BuildStageStatuses(
        string stageDir,
        string readinessSummary,
        BootstrapStoreSummary bootstrapStore,
        RefreshPlanSummary refreshPlan,
        ModelRecommendationSummary modelRecommendation,
        bool hasPredictions,
        bool hasSavedRuns)
    {
        var results = new List<WorkspaceStageInfo>
        {
            new WorkspaceStageInfo
            {
                StageKey = "Build local bootstrap store",
                Status = bootstrapStore.IsPresent ? "completed" : "pending",
                Note = bootstrapStore.IsPresent
                    ? $"Indexed bootstrap store ready with {bootstrapStore.RecordCount:N0} PDB records."
                    : "Build the local bootstrap store once to cache per-PDB facts before dataset design."
            },
            BuildAggregateStage(
                stageDir,
                "Preview representative coverage",
                "Representative search state is grounded in the workspace readiness snapshot.",
                "audit",
                "report-source-capabilities",
                "report"),
            BuildAggregateStage(
                stageDir,
                "Assemble graph-ready data",
                "Graph-ready extraction is built from extract, graph, and feature pipeline stages.",
                "extract",
                "build-graph",
                "build-structural-graphs",
                "engineer-dataset",
                "run-feature-pipeline",
                "build-training-examples"),
            BuildAggregateStage(
                stageDir,
                "Design the split",
                "Split state is read from the current split and custom training set builders.",
                "build-splits",
                "build-custom-training-set"),
            new WorkspaceStageInfo
            {
                StageKey = "Plan selected-PDB refresh",
                Status = refreshPlan.IsPresent ? "completed" : "pending",
                Note = refreshPlan.IsPresent
                    ? $"Targeted refresh plan prepared for {refreshPlan.RecordCount:N0} selected PDB IDs."
                    : "Generate a targeted refresh plan after curating the active training set."
            },
            new WorkspaceStageInfo
            {
                StageKey = "Recommend model",
                Status = hasSavedRuns || modelRecommendation.IsPresent ? "completed" : "pending",
                Note = hasSavedRuns
                    ? "Saved model runs were detected, so Model Studio can compare concrete backend choices."
                    : modelRecommendation.IsPresent
                        ? modelRecommendation.NextAction
                        : "No saved model runs detected yet. Model Studio will populate once training artifacts land."
            },
            BuildAggregateStage(
                stageDir,
                "Train candidate model",
                "Training state is read from the baseline training and evaluation stages.",
                "train-recommended-model",
                "train-baseline-model",
                "evaluate-baseline-model"),
            new WorkspaceStageInfo
            {
                StageKey = "Run inference",
                Status = hasPredictions ? "completed" : "pending",
                Note = hasPredictions
                    ? "Saved prediction manifests were detected under data/prediction and are now driving the inference view."
                    : "No saved prediction manifests detected yet."
            },
        };

        if (!string.IsNullOrWhiteSpace(readinessSummary))
        {
            results.Insert(0, new WorkspaceStageInfo
            {
                StageKey = "Frame the story",
                Status = "completed",
                Note = readinessSummary,
            });
        }

        return results;
    }

    private static double Clamp01(double value) => Math.Max(0.0, Math.Min(1.0, value));

    private static GraphPackageSummary ReadLatestGraphPackageSummary(string graphsRoot)
    {
        var manifestPath = FindLatestGraphManifest(graphsRoot);
        if (!File.Exists(manifestPath))
        {
            return new GraphPackageSummary(false, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, Array.Empty<string>(), 0, 0, 0, 0);
        }

        var raw = ReadObject(manifestPath);
        var graphs = raw.TryGetValue("graphs", out var graphsValue) && graphsValue is IEnumerable<object?> items
            ? items.OfType<Dictionary<string, object?>>().ToList()
            : new List<Dictionary<string, object?>>();
        var exportFormats = ReadStringList(raw, "export_formats");

        return new GraphPackageSummary(
            true,
            manifestPath,
            ReadString(raw, "graph_level", "unknown"),
            ReadString(raw, "scope", "unknown"),
            ReadString(raw, "selection", "unknown"),
            ReadString(raw, "generated_at", string.Empty),
            exportFormats,
            GetInt(raw, "selected_count"),
            GetInt(raw, "processed_count"),
            GetInt(raw, "skipped_count"),
            graphs.Count);
    }

    private static string FindLatestGraphManifest(string graphsRoot)
    {
        if (!Directory.Exists(graphsRoot))
        {
            return string.Empty;
        }

        return Directory
            .GetFiles(graphsRoot, "graph_manifest.json", SearchOption.AllDirectories)
            .OrderByDescending(File.GetLastWriteTimeUtc)
            .FirstOrDefault()
            ?? string.Empty;
    }

    private static DatasetGraphSummary ReadLatestDatasetGraphSummary(string datasetsRoot)
    {
        if (!Directory.Exists(datasetsRoot))
        {
            return new DatasetGraphSummary(false, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, Array.Empty<string>(), 0, 0.0);
        }

        var graphConfigPath = Directory
            .GetFiles(datasetsRoot, "graph_config.json", SearchOption.AllDirectories)
            .OrderByDescending(File.GetLastWriteTimeUtc)
            .FirstOrDefault();
        if (string.IsNullOrWhiteSpace(graphConfigPath))
        {
            return new DatasetGraphSummary(false, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, Array.Empty<string>(), 0, 0.0);
        }

        var diversityReportPath = Path.Combine(Path.GetDirectoryName(graphConfigPath) ?? string.Empty, "diversity_report.json");
        var graphConfig = ReadObject(graphConfigPath);
        var exportFormats = ReadStringList(graphConfig, "graph_export_formats");

        return new DatasetGraphSummary(
            true,
            graphConfigPath,
            diversityReportPath,
            Path.GetFileName(Path.GetDirectoryName(graphConfigPath) ?? string.Empty),
            ReadString(graphConfig, "graph_level", string.Empty),
            ReadString(graphConfig, "graph_scope", string.Empty),
            ReadString(graphConfig, "graph_selection", string.Empty),
            exportFormats,
            GetInt(graphConfig, "graph_covered_rows"),
            GetDouble(graphConfig, "graph_covered_fraction"));
    }

    private static ModelRecommendationSummary ReadModelRecommendationSummary(string jsonPath, string markdownPath)
    {
        if (!File.Exists(jsonPath))
        {
            return new ModelRecommendationSummary(false, jsonPath, markdownPath, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, 0.0, 0);
        }

        var raw = ReadObject(jsonPath);
        var topRecommendation = ReadObjectFromObject(raw, "top_recommendation");
        var datasetProfile = ReadObjectFromObject(raw, "dataset_profile");

        return new ModelRecommendationSummary(
            true,
            jsonPath,
            markdownPath,
            ReadString(raw, "status", string.Empty),
            ReadString(raw, "summary", string.Empty),
            ReadString(raw, "next_action", string.Empty),
            ReadString(topRecommendation, "label", string.Empty),
            ReadString(topRecommendation, "family", string.Empty),
            ReadString(topRecommendation, "modality", string.Empty),
            ReadString(topRecommendation, "why_it_fits", string.Empty),
            ReadString(raw, "starter_config_path", string.Empty),
            ReadString(datasetProfile, "dataset_source", string.Empty),
            GetDouble(datasetProfile, "graph_covered_fraction"),
            GetInt(datasetProfile, "graph_covered_rows"));
    }

    private static RuntimeEnvironmentSummary ReadRuntimeEnvironmentSummary(string root)
    {
        var venvPython = Path.Combine(root, ".venv", "Scripts", "python.exe");
        var sitePackages = Path.Combine(root, ".venv", "Lib", "site-packages");
        var hasLocalVenv = File.Exists(venvPython);

        bool HasPackage(string name) =>
            Directory.Exists(Path.Combine(sitePackages, name))
            || File.Exists(Path.Combine(sitePackages, $"{name}.py"))
            || Directory.GetFiles(sitePackages, $"{name}*.dist-info", SearchOption.TopDirectoryOnly).Any()
            || Directory.GetFiles(sitePackages, $"{name}*.libs", SearchOption.TopDirectoryOnly).Any();

        var hasSklearn = Directory.Exists(sitePackages) && HasPackage("sklearn");
        var hasTorch = Directory.Exists(sitePackages) && HasPackage("torch");
        var hasTorchGeometric = Directory.Exists(sitePackages) && (HasPackage("torch_geometric") || HasPackage("torch-geometric"));
        var hasXgboost = Directory.Exists(sitePackages) && HasPackage("xgboost");
        var hasPyarrow = Directory.Exists(sitePackages) && HasPackage("pyarrow");
        var hasFastparquet = Directory.Exists(sitePackages) && HasPackage("fastparquet");
        var hasEsm = Directory.Exists(sitePackages) && HasPackage("esm");

        var labels = new List<string>();
        if (hasSklearn)
        {
            labels.Add("sklearn");
        }
        if (hasTorch)
        {
            labels.Add("torch");
        }
        if (hasTorchGeometric)
        {
            labels.Add("torch_geometric");
        }
        if (hasXgboost)
        {
            labels.Add("xgboost");
        }

        return new RuntimeEnvironmentSummary(
            hasLocalVenv,
            hasLocalVenv ? venvPython : "system python",
            hasSklearn,
            hasTorch,
            hasTorchGeometric,
            hasXgboost,
            hasPyarrow,
            hasFastparquet,
            hasEsm,
            labels.Count > 0 ? string.Join(", ", labels) : "none detected");
    }

    private static IReadOnlyList<StatCard> BuildEnvironmentStats(
        string root,
        RuntimeEnvironmentSummary runtime)
    {
        var graphNativeValue = runtime.HasTorch && runtime.HasTorchGeometric
            ? "Native"
            : runtime.HasTorch || runtime.HasSklearn
                ? "Fallback"
                : "Blocked";
        var dataExtras = new List<string>();
        if (runtime.HasPyarrow)
        {
            dataExtras.Add("pyarrow");
        }
        if (runtime.HasFastparquet)
        {
            dataExtras.Add("fastparquet");
        }
        if (runtime.HasEsm)
        {
            dataExtras.Add("esm");
        }

        return new[]
        {
            new StatCard
            {
                Label = "Python runtime",
                Value = runtime.UsesLocalVenv ? "Local .venv" : "System",
                Caption = $"Resolved interpreter: {runtime.PythonPath}",
            },
            new StatCard
            {
                Label = "Model backends",
                Value = runtime.InstalledBackendsLabel,
                Caption = "Local executable backends detected for training and fallback planning.",
            },
            new StatCard
            {
                Label = "Graph-native path",
                Value = graphNativeValue,
                Caption = graphNativeValue == "Native"
                    ? "Torch and torch_geometric are both present, so native graph training is available."
                    : graphNativeValue == "Fallback"
                        ? "The machine can still execute a fallback path, but the full graph-native stack is incomplete."
                        : "Install torch plus torch_geometric to unlock native graph execution.",
            },
            new StatCard
            {
                Label = "Data extras",
                Value = dataExtras.Count > 0 ? string.Join(", ", dataExtras) : "none",
                Caption = "Optional extras used for parquet/export acceleration and embedding-heavy workflows.",
            },
            new StatCard
            {
                Label = "WinUI launch path",
                Value = File.Exists(Path.Combine(root, "Launch PBData WinUI.bat")) ? "Ready" : "Missing",
                Caption = "Fresh-clone launcher used for the Windows desktop shell.",
            },
        };
    }

    private static string BuildEnvironmentGuidance(RuntimeEnvironmentSummary runtime)
    {
        var notes = new List<string>();

        if (!(runtime.HasTorch && runtime.HasTorchGeometric))
        {
            notes.Add("Native graph training is not fully enabled. Install torch-geometric on top of torch to unlock the graph-native path.");
        }
        if (!runtime.HasSklearn)
        {
            notes.Add("Tree-based recommended models may fall back until scikit-learn is installed.");
        }
        if (!(runtime.HasPyarrow || runtime.HasFastparquet))
        {
            notes.Add("Parquet acceleration is unavailable. Install pyarrow for faster export-heavy stages.");
        }
        if (!runtime.HasEsm)
        {
            notes.Add("Embedding-heavy sequence workflows still need the optional esm package.");
        }

        return notes.Count > 0
            ? string.Join(" ", notes)
            : "This machine is ready for the main workspace, training, graph, and export paths with no obvious optional capability gaps.";
    }

    private static string BuildEnvironmentFixCommands(RuntimeEnvironmentSummary runtime)
    {
        var commands = new List<string>();

        if (!runtime.HasSklearn)
        {
            commands.Add("python -m pip install scikit-learn");
        }
        if (!runtime.HasTorch)
        {
            commands.Add("python -m pip install torch");
        }
        if (runtime.HasTorch && !runtime.HasTorchGeometric)
        {
            commands.Add("python -m pip install torch-geometric");
        }
        if (!(runtime.HasPyarrow || runtime.HasFastparquet))
        {
            commands.Add("python -m pip install pyarrow");
        }
        if (!runtime.HasEsm)
        {
            commands.Add("python -m pip install fair-esm");
        }

        return commands.Count > 0
            ? string.Join(Environment.NewLine, commands)
            : "No install commands are currently needed for the main local workflow.";
    }

    private static string BuildPresentationSummary(string summary, IReadOnlyCollection<string> warnings)
    {
        if (warnings.Contains("demo_mode_simulated_outputs", StringComparer.OrdinalIgnoreCase))
        {
            return "This workspace is arranged to present the full product story clearly, from local data foundation and dataset design through model strategy, training evidence, and explainable inference.";
        }

        return summary;
    }

    private static string DescribeRecommendationRuntime(
        ModelRecommendationSummary recommendation,
        RuntimeEnvironmentSummary runtime)
    {
        if (!recommendation.IsPresent || string.IsNullOrWhiteSpace(recommendation.Family))
        {
            return "Unknown";
        }

        return recommendation.Family.ToLowerInvariant() switch
        {
            "random_forest" => runtime.HasSklearn ? "Native" : runtime.HasTorch ? "Fallback" : "Blocked",
            "xgboost" => runtime.HasXgboost ? "Native" : runtime.HasSklearn ? "Fallback" : runtime.HasTorch ? "Fallback" : "Blocked",
            "dense_nn" => runtime.HasTorch ? "Native" : runtime.HasSklearn ? "Fallback" : "Blocked",
            "gnn" => runtime.HasTorch && runtime.HasTorchGeometric ? "Native" : runtime.HasTorch || runtime.HasSklearn ? "Fallback" : "Blocked",
            "hybrid_fusion" => runtime.HasTorch && runtime.HasTorchGeometric ? "Native" : runtime.HasTorch || runtime.HasSklearn ? "Fallback" : "Blocked",
            _ => runtime.HasTorch || runtime.HasSklearn ? "Fallback" : "Blocked",
        };
    }

    private static string BuildRuntimeCaption(
        ModelRecommendationSummary recommendation,
        RuntimeEnvironmentSummary runtime,
        string runtimeMode)
    {
        var label = string.IsNullOrWhiteSpace(recommendation.Label) ? recommendation.Family : recommendation.Label;
        return runtimeMode switch
        {
            "Native" => $"{label} can run directly on this machine with the currently detected backends.",
            "Fallback" => $"{label} will train with a runtime-adjusted fallback on this machine because the preferred backend stack is incomplete.",
            "Blocked" => $"{label} is currently blocked by missing local backends in {runtime.PythonPath}.",
            _ => $"{label} runtime readiness has not been established yet."
        };
    }

    private static List<Dictionary<string, object?>> ReadArray(string path)
    {
        if (!File.Exists(path))
        {
            return new List<Dictionary<string, object?>>();
        }
        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(path));
            if (document.RootElement.ValueKind != JsonValueKind.Array)
            {
                return new List<Dictionary<string, object?>>();
            }

            return document.RootElement
                .EnumerateArray()
                .Where(item => item.ValueKind == JsonValueKind.Object)
                .Select(ConvertObject)
                .ToList();
        }
        catch
        {
            return new List<Dictionary<string, object?>>();
        }
    }

    private static WorkspaceStageInfo BuildAggregateStage(
        string stageDir,
        string stageKey,
        string fallbackNote,
        params string[] stageFiles)
    {
        var allPayloads = stageFiles
            .Select(stageFile => ReadObject(Path.Combine(stageDir, $"{stageFile}.json")))
            .Where(payload => payload.Count > 0)
            .ToList();
        var payloads = allPayloads
            .Where(payload => !IsSimulatedPayload(payload))
            .ToList();

        if (allPayloads.Count == 0)
        {
            return new WorkspaceStageInfo
            {
                StageKey = stageKey,
                Status = "pending",
                Note = "No stage-state files detected yet.",
            };
        }

        if (payloads.Count == 0)
        {
            return new WorkspaceStageInfo
            {
                StageKey = stageKey,
                Status = "pending",
                Note = "Curated presentation state is available for this step. Run the workflow path to replace it with live workspace state.",
            };
        }

        var status = payloads.Any(payload => ReadString(payload, "status", "").Equals("failed", StringComparison.OrdinalIgnoreCase))
            ? "failed"
            : payloads.Any(payload => ReadString(payload, "status", "").Equals("running", StringComparison.OrdinalIgnoreCase))
                ? "running"
                : payloads.Any(payload => ReadString(payload, "status", "").Equals("completed_with_failures", StringComparison.OrdinalIgnoreCase))
                    ? "completed_with_failures"
                    : payloads.All(payload => ReadString(payload, "status", "").Equals("completed", StringComparison.OrdinalIgnoreCase))
                        ? "completed"
                        : "pending";

        var note = payloads
            .Select(payload => ReadString(payload, "notes", ""))
            .FirstOrDefault(value => !string.IsNullOrWhiteSpace(value))
            ?? fallbackNote;

        return new WorkspaceStageInfo
        {
            StageKey = stageKey,
            Status = status,
            Note = note,
        };
    }

    private static bool IsSimulatedPayload(Dictionary<string, object?> payload)
    {
        return payload.TryGetValue("simulated", out var simulated)
            && simulated is bool simulatedBool
            && simulatedBool;
    }

    private static string HumanizeFamily(string family)
    {
        if (string.IsNullOrWhiteSpace(family))
        {
            return "Saved model";
        }

        return string.Join(" ",
            family
                .Split('_', StringSplitOptions.RemoveEmptyEntries)
                .Select(part => char.ToUpperInvariant(part[0]) + part[1..]));
    }

    private static string HumanizeExecutionStrategy(string strategy)
    {
        return strategy.ToLowerInvariant() switch
        {
            "prefer_native" => "Native only",
            "safe_baseline" => "Safe baseline",
            _ => "Automatic",
        };
    }

    private static BootstrapStoreSummary ReadBootstrapStoreSummary(string manifestPath, string databasePath)
    {
        if (!File.Exists(manifestPath))
        {
            return new BootstrapStoreSummary(false, manifestPath, databasePath, 0, 0, string.Empty);
        }

        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(manifestPath));
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                return new BootstrapStoreSummary(false, manifestPath, databasePath, 0, 0, string.Empty);
            }

            var root = document.RootElement;
            var recordCount = TryGetInt(root, "record_count");
            var generatedAt = TryGetString(root, "generated_at");
            var sourceInputs = root.TryGetProperty("source_inputs", out var sourceInputsElement) && sourceInputsElement.ValueKind == JsonValueKind.Object
                ? sourceInputsElement.EnumerateObject().Count()
                : 0;

            return new BootstrapStoreSummary(true, manifestPath, databasePath, recordCount, sourceInputs, generatedAt);
        }
        catch
        {
            return new BootstrapStoreSummary(false, manifestPath, databasePath, 0, 0, string.Empty);
        }
    }

    private static RefreshPlanSummary ReadRefreshPlanSummary(string manifestPath)
    {
        if (!File.Exists(manifestPath))
        {
            return new RefreshPlanSummary(false, manifestPath, string.Empty, 0, string.Empty);
        }

        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(manifestPath));
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                return new RefreshPlanSummary(false, manifestPath, string.Empty, 0, string.Empty);
            }

            var root = document.RootElement;
            return new RefreshPlanSummary(
                true,
                manifestPath,
                TryGetString(root, "selected_source"),
                TryGetInt(root, "record_count"),
                TryGetString(root, "generated_at"));
        }
        catch
        {
            return new RefreshPlanSummary(false, manifestPath, string.Empty, 0, string.Empty);
        }
    }

    private static string FormatTimestamp(string timestamp)
    {
        return DateTimeOffset.TryParse(timestamp, out var parsed)
            ? parsed.ToLocalTime().ToString("yyyy-MM-dd HH:mm")
            : "Unknown";
    }

    private static int TryGetInt(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return 0;
        }

        return property.ValueKind switch
        {
            JsonValueKind.Number when property.TryGetInt32(out var value) => value,
            JsonValueKind.Number when property.TryGetInt64(out var longValue) => (int)longValue,
            JsonValueKind.String when int.TryParse(property.GetString(), out var parsed) => parsed,
            _ => 0,
        };
    }

    private static string TryGetString(JsonElement element, string propertyName)
    {
        return element.TryGetProperty(propertyName, out var property) && property.ValueKind == JsonValueKind.String
            ? property.GetString() ?? string.Empty
            : string.Empty;
    }

    private static string HumanizeReadinessLabel(string label)
    {
        if (string.IsNullOrWhiteSpace(label))
        {
            return string.Empty;
        }

        return label.ToLowerInvariant() switch
        {
            "ready_for_internal_demo" => "Presentation workspace ready",
            "ready" => "Workspace ready",
            "workspace_detected" => "Workspace detected",
            _ => string.Join(" ",
                label
                    .Split('_', StringSplitOptions.RemoveEmptyEntries)
                    .Select(part => char.ToUpperInvariant(part[0]) + part[1..]))
        };
    }
}
