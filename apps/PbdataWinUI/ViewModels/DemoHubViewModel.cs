using System.Collections.ObjectModel;
using System.Linq;

namespace PbdataWinUI.ViewModels;

public sealed partial class DemoHubViewModel : BaseViewModel
{
    private readonly WorkspaceDataService _workspaceDataService = new();
    private readonly WorkflowCommandService _workflowCommandService = new();
    private readonly DispatcherQueue _dispatcherQueue = DispatcherQueue.GetForCurrentThread();
    private readonly List<DemoStep> _steps;
    private Dictionary<string, WorkspaceStageInfo> _workspaceStageLookup = new(StringComparer.OrdinalIgnoreCase);

    private string _selectedDatasetProfileKey = "broad";
    private string _selectedBalanceKey = "family";
    private string _selectedEmbeddingKey = "esm2";
    private string _selectedObjectiveKey = "hybrid";
    private string _selectedModelFamilyKey = "hybrid";
    private string _selectedModalityKey = "tri_modal";
    private string _selectedSpeedKey = "balanced";
    private string _selectedTrainingExecutionModeKey = "auto";
    private string _selectedInferenceScenarioKey = "novel_pocket";
    private string _selectedGraphLevelKey = "residue";
    private string _selectedGraphScopeKey = "interface_only";
    private string _selectedGraphExportKey = "training";
    private string _selectedGraphTargetKey = "refresh_plan";
    private int _currentStepIndex;
    private int _completedStepCount;

    private string _selectedModelHeadline = string.Empty;
    private string _selectedModelPitch = string.Empty;
    private string _bootstrapNarrative = string.Empty;
    private string _refreshPlanNarrative = string.Empty;
    private string _graphPackageNarrative = string.Empty;
    private string _datasetNarrative = string.Empty;
    private string _graphDesignNarrative = string.Empty;
    private string _splitNarrative = string.Empty;
    private string _inferenceNarrative = string.Empty;
    private string _runSummary = string.Empty;
    private string _selectedRunName = "Hybrid Fusion - novelty holdout";
    private string _trainingExecutionNarrative = string.Empty;
    private string _environmentGuidance = string.Empty;
    private string _environmentFixCommands = string.Empty;
    private string _demoHeadline = string.Empty;
    private string _demoDisclaimer = string.Empty;
    private string _workspaceRootInput = string.Empty;
    private string _workspaceSummary = string.Empty;
    private bool _isWorkflowBusy;
    private string _workflowStatus = "Idle";
    private string _activeWorkflowLabel = "No command running";
    private string _lastWorkflowCommand = "No command run yet";

    private PointCollection _trainingCurvePoints = new();
    private PointCollection _validationCurvePoints = new();

    public DemoHubViewModel()
    {
        Title = "Guided Workflow";

        DatasetProfiles = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "broad", Label = "Broad Discovery Panel", Caption = "Balanced across protein-protein, protein-ligand, and control contexts." },
            new() { Key = "ppi", Label = "Protein-Protein Focus", Caption = "Bias toward interfaces, multimer assemblies, and interface stability questions." },
            new() { Key = "ligand", Label = "Ligand-Enriched Screen", Caption = "Emphasize ligand contact patterns, assay context, and pocket diversity." },
            new() { Key = "single", Label = "Single-Protein Controls", Caption = "Useful for negative controls, background properties, and baseline calibration." },
        };

        BalanceStrategies = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "family", Label = "Family-balanced", Caption = "Controls sequence identity, fold families, and motif over-representation." },
            new() { Key = "novelty", Label = "Novelty-seeking", Caption = "Pushes rarer folds and under-sampled chemotypes into training." },
            new() { Key = "assay", Label = "Assay-balanced", Caption = "Keeps measurement context from collapsing into a single assay bias." },
        };

        EmbeddingStrategies = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "esm2", Label = "ESM2", Caption = "Strong default when sequence context should complement structure and assays." },
            new() { Key = "prott5", Label = "ProtT5", Caption = "Useful when long-range sequence semantics matter for generalization." },
            new() { Key = "none", Label = "None", Caption = "Feature-only path for fast baseline comparisons." },
        };

        Objectives = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "hybrid", Label = "Hybrid Binding Ranking", Caption = "Score likely binding strength while reconciling assay and structure evidence." },
            new() { Key = "interface", Label = "Interface Classification", Caption = "Classify contact-rich protein interaction surfaces." },
            new() { Key = "screen", Label = "Pocket Screening", Caption = "Prioritize ligand-pocket compatibility across many candidates." },
        };

        ModelFamilies = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "auto", Label = "Auto path", Caption = "Let the platform recommend the most suitable model family." },
            new() { Key = "rf", Label = "Random Forest", Caption = "Fast, interpretable tree baseline for tabular engineered features." },
            new() { Key = "xgb", Label = "XGBoost", Caption = "Stronger tabular learner with robust ranking performance." },
            new() { Key = "gnn", Label = "Graph Neural Net", Caption = "Focus on residue-contact topology and graph-derived neighborhoods." },
            new() { Key = "hybrid", Label = "Hybrid Fusion", Caption = "Blend graph, tabular, and embedding signals into one scored model." },
        };

        Modalities = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "tri_modal", Label = "Structure + sequence + assay", Caption = "Best for realistic, cross-source training previews." },
            new() { Key = "graph_only", Label = "Structure graph only", Caption = "Useful when graph topology is the main story." },
            new() { Key = "tabular", Label = "Descriptors + assay context", Caption = "A lighter-weight path that still tells a coherent story." },
        };

        SpeedProfiles = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "fast", Label = "Fast preview", Caption = "Shortest preview train time and lighter output." },
            new() { Key = "balanced", Label = "Balanced", Caption = "Good tradeoff between explanation depth and accuracy." },
            new() { Key = "best", Label = "Best accuracy", Caption = "Longest profile with the strongest cross-family generalization target." },
        };

        TrainingExecutionModes = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "auto", Label = "Automatic", Caption = "Use the recommended path and allow a backend-aware fallback if the native stack is unavailable." },
            new() { Key = "prefer_native", Label = "Native only", Caption = "Fail rather than silently fallback when the recommended family cannot run natively on this machine." },
            new() { Key = "safe_baseline", Label = "Safe baseline", Caption = "Prefer the most executable baseline path for this runtime, even if it is more conservative than the recommendation." },
        };

        InferenceScenarios = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "novel_pocket", Label = "Novel pocket screen", Caption = "Demonstrate how the saved model ranks new complexes." },
            new() { Key = "ppi_triage", Label = "PPI triage", Caption = "Rank interface-heavy protein-protein examples." },
            new() { Key = "mutant_scan", Label = "Mutant sensitivity", Caption = "Show how motif and residue changes shift predictions." },
        };

        GraphLevels = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "residue", Label = "Residue graph", Caption = "Compact residue-contact representation for scalable interface and pocket modeling." },
            new() { Key = "atom", Label = "Atom graph", Caption = "Finer-grained atomic geometry for chemistry-heavy structure reasoning." },
        };

        GraphScopes = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "whole_protein", Label = "Whole protein", Caption = "Preserve global topology across the full experimental structure." },
            new() { Key = "interface_only", Label = "Interface only", Caption = "Focus directly on the interacting chains and hotspot residues." },
            new() { Key = "shell", Label = "Neighborhood shell", Caption = "Crop a configurable shell around the binding interface or pocket." },
        };

        GraphExportBundles = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "training", Label = "PyG + NX", Caption = "Good default for message-passing training plus transparent JSON inspection." },
            new() { Key = "interop", Label = "PyG + DGL + NX", Caption = "Keep all export targets available for experimentation and porting." },
            new() { Key = "audit", Label = "NX only", Caption = "Lighter export path for inspection, diagnostics, and graph review." },
        };

        GraphBuildTargets = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "refresh_plan", Label = "Selected refresh plan", Caption = "Build graph packages for the targeted selected-PDB manifest so the main curation set stays responsive." },
            new() { Key = "training_set", Label = "Current training set", Caption = "Use the current curated training CSV when graph packages should follow the active dataset." },
            new() { Key = "preview", Label = "Preview subset", Caption = "Materialize a small first-pass slice so a fresh clone can prove the path quickly." },
            new() { Key = "all", Label = "All local structures", Caption = "Build the entire currently extracted structure pool when you want a full graph cache." },
        };

        _steps = new List<DemoStep>
        {
            new() { Number = 1, PageKey = "Guide", Title = "Frame the story", ActionLabel = "Begin workflow", WhyItMatters = "This primes the workspace with a coherent scientific story instead of a pile of controls.", WhatToClick = "Begin workflow", HowToFindIt = "Open Workflow and use the first primary button." },
            new() { Number = 2, PageKey = "Guide", Title = "Build local bootstrap store", ActionLabel = "Build bootstrap", WhyItMatters = "This front-loads broad PDB-linked data into a fast local store so later curation and training-set design stay responsive.", WhatToClick = "Build bootstrap store", HowToFindIt = "Stay on Workflow and use the bootstrap action in the command bar." },
            new() { Number = 3, PageKey = "Dataset", Title = "Preview representative coverage", ActionLabel = "Preview search", WhyItMatters = "The platform shows broad structural coverage rather than returning a narrow cluster of near-duplicates.", WhatToClick = "Preview Search", HowToFindIt = "Open Dataset and use the first action button." },
            new() { Number = 4, PageKey = "Dataset", Title = "Assemble the graph-ready set", ActionLabel = "Assemble set", WhyItMatters = "This reconciles structure, assay, sequence, and motif context into a balanced product-ready dataset.", WhatToClick = "Assemble Set", HowToFindIt = "Stay in Dataset and use the dataset-build action." },
            new() { Number = 5, PageKey = "Dataset", Title = "Design the leakage-resistant split", ActionLabel = "Design split", WhyItMatters = "The split logic emphasizes family holdout, motif grouping, and source-aware evaluation.", WhatToClick = "Design Split", HowToFindIt = "Use the split action in Dataset." },
            new() { Number = 6, PageKey = "Dataset", Title = "Plan selected-PDB refresh", ActionLabel = "Plan refresh", WhyItMatters = "After the active set is chosen, the platform can recheck only those selected PDB IDs instead of the full corpus.", WhatToClick = "Plan Refresh", HowToFindIt = "Stay in Dataset and use the targeted refresh action." },
            new() { Number = 7, PageKey = "Model", Title = "Refresh the model path", ActionLabel = "Recommend", WhyItMatters = "Model Studio explains why a given architecture suits the selected modalities and objective.", WhatToClick = "Recommend", HowToFindIt = "Open Models and use the first action button." },
            new() { Number = 8, PageKey = "Model", Title = "Train the candidate", ActionLabel = "Train", WhyItMatters = "The training view shows realistic curves, scores, and architecture tradeoffs.", WhatToClick = "Train", HowToFindIt = "Use the primary training button in Models." },
            new() { Number = 9, PageKey = "Inference", Title = "Run saved-model inference", ActionLabel = "Run inference", WhyItMatters = "This demonstrates how the platform turns a saved model into ranked, explainable predictions.", WhatToClick = "Run Inference", HowToFindIt = "Open Inference and use the primary inference button." },
        };

        TimelineStages = new ObservableCollection<DemoTimelineStage>();
        BootstrapStats = new ObservableCollection<StatCard>();
        RefreshPlanStats = new ObservableCollection<StatCard>();
        GraphPackageStats = new ObservableCollection<StatCard>();
        DatasetStats = new ObservableCollection<StatCard>();
        EnvironmentStats = new ObservableCollection<StatCard>();
        GraphDesignStats = new ObservableCollection<StatCard>();
        TrainingStats = new ObservableCollection<StatCard>();
        MetricBars = new ObservableCollection<MetricBar>();
        ArchitectureStages = new ObservableCollection<ArchitectureStage>();
        ComparisonRuns = new ObservableCollection<RunSummary>();
        ArtifactSummaries = new ObservableCollection<ArtifactSummary>();
        Predictions = new ObservableCollection<PredictionSummary>();
        ActivityLog = new ObservableCollection<string>();
        WorkflowConsoleLines = new ObservableCollection<string>();

        _workspaceRootInput = _workspaceDataService.DetectWorkspaceRoot();
        ResetDemo();
    }

    public ObservableCollection<ChoiceItem> DatasetProfiles { get; }
    public ObservableCollection<ChoiceItem> BalanceStrategies { get; }
    public ObservableCollection<ChoiceItem> EmbeddingStrategies { get; }
    public ObservableCollection<ChoiceItem> Objectives { get; }
    public ObservableCollection<ChoiceItem> ModelFamilies { get; }
    public ObservableCollection<ChoiceItem> Modalities { get; }
    public ObservableCollection<ChoiceItem> SpeedProfiles { get; }
    public ObservableCollection<ChoiceItem> TrainingExecutionModes { get; }
    public ObservableCollection<ChoiceItem> InferenceScenarios { get; }
    public ObservableCollection<ChoiceItem> GraphLevels { get; }
    public ObservableCollection<ChoiceItem> GraphScopes { get; }
    public ObservableCollection<ChoiceItem> GraphExportBundles { get; }
    public ObservableCollection<ChoiceItem> GraphBuildTargets { get; }

    public ObservableCollection<DemoTimelineStage> TimelineStages { get; }
    public ObservableCollection<StatCard> BootstrapStats { get; }
    public ObservableCollection<StatCard> RefreshPlanStats { get; }
    public ObservableCollection<StatCard> GraphPackageStats { get; }
    public ObservableCollection<StatCard> DatasetStats { get; }
    public ObservableCollection<StatCard> EnvironmentStats { get; }
    public ObservableCollection<StatCard> GraphDesignStats { get; }
    public ObservableCollection<StatCard> TrainingStats { get; }
    public ObservableCollection<MetricBar> MetricBars { get; }
    public ObservableCollection<ArchitectureStage> ArchitectureStages { get; }
    public ObservableCollection<RunSummary> ComparisonRuns { get; }
    public ObservableCollection<ArtifactSummary> ArtifactSummaries { get; }
    public ObservableCollection<PredictionSummary> Predictions { get; }
    public ObservableCollection<string> ActivityLog { get; }
    public ObservableCollection<string> WorkflowConsoleLines { get; }

    public string SelectedDatasetProfileKey
    {
        get => _selectedDatasetProfileKey;
        set
        {
            if (SetProperty(ref _selectedDatasetProfileKey, value))
            {
                RefreshDatasetProjection();
            }
        }
    }

    public string SelectedBalanceKey
    {
        get => _selectedBalanceKey;
        set
        {
            if (SetProperty(ref _selectedBalanceKey, value))
            {
                RefreshDatasetProjection();
            }
        }
    }

    public string SelectedEmbeddingKey
    {
        get => _selectedEmbeddingKey;
        set
        {
            if (SetProperty(ref _selectedEmbeddingKey, value))
            {
                RefreshDatasetProjection();
                RefreshModelProjection();
            }
        }
    }

    public string SelectedObjectiveKey
    {
        get => _selectedObjectiveKey;
        set
        {
            if (SetProperty(ref _selectedObjectiveKey, value))
            {
                RefreshDatasetProjection();
                RefreshModelProjection();
            }
        }
    }

    public string SelectedModelFamilyKey
    {
        get => _selectedModelFamilyKey;
        set
        {
            if (SetProperty(ref _selectedModelFamilyKey, value))
            {
                RefreshModelProjection();
            }
        }
    }

    public string SelectedModalityKey
    {
        get => _selectedModalityKey;
        set
        {
            if (SetProperty(ref _selectedModalityKey, value))
            {
                RefreshModelProjection();
            }
        }
    }

    public string SelectedSpeedKey
    {
        get => _selectedSpeedKey;
        set
        {
            if (SetProperty(ref _selectedSpeedKey, value))
            {
                RefreshModelProjection();
            }
        }
    }

    public string SelectedTrainingExecutionModeKey
    {
        get => _selectedTrainingExecutionModeKey;
        set
        {
            if (SetProperty(ref _selectedTrainingExecutionModeKey, value))
            {
                RefreshTrainingExecutionNarrative();
            }
        }
    }

    public string SelectedInferenceScenarioKey
    {
        get => _selectedInferenceScenarioKey;
        set
        {
            if (SetProperty(ref _selectedInferenceScenarioKey, value))
            {
                RefreshInferenceProjection();
            }
        }
    }

    public string SelectedGraphLevelKey
    {
        get => _selectedGraphLevelKey;
        set
        {
            if (SetProperty(ref _selectedGraphLevelKey, value))
            {
                RefreshGraphProjection();
                RefreshModelProjection();
            }
        }
    }

    public string SelectedGraphScopeKey
    {
        get => _selectedGraphScopeKey;
        set
        {
            if (SetProperty(ref _selectedGraphScopeKey, value))
            {
                RefreshGraphProjection();
                RefreshModelProjection();
            }
        }
    }

    public string SelectedGraphExportKey
    {
        get => _selectedGraphExportKey;
        set
        {
            if (SetProperty(ref _selectedGraphExportKey, value))
            {
                RefreshGraphProjection();
            }
        }
    }

    public string SelectedGraphTargetKey
    {
        get => _selectedGraphTargetKey;
        set
        {
            if (SetProperty(ref _selectedGraphTargetKey, value))
            {
                RefreshGraphProjection();
            }
        }
    }

    public string CurrentStepTitle => _steps[_currentStepIndex].Title;
    public string CurrentStepWhy => _steps[_currentStepIndex].WhyItMatters;
    public string CurrentStepClick => _steps[_currentStepIndex].WhatToClick;
    public string CurrentStepFind => _steps[_currentStepIndex].HowToFindIt;
    public string CurrentRecommendedPageKey => _steps[_currentStepIndex].PageKey;
    public string CurrentActionLabel => _steps[_currentStepIndex].ActionLabel;
    public string ProgressLabel => $"{_completedStepCount} of {_steps.Count} guided steps completed";
    public string DemoHeadline
    {
        get => _demoHeadline;
        private set => SetProperty(ref _demoHeadline, value);
    }

    public string DemoDisclaimer
    {
        get => _demoDisclaimer;
        private set => SetProperty(ref _demoDisclaimer, value);
    }

    public string WorkspaceRootInput
    {
        get => _workspaceRootInput;
        set => SetProperty(ref _workspaceRootInput, value);
    }

    public string WorkspaceSummary
    {
        get => _workspaceSummary;
        private set => SetProperty(ref _workspaceSummary, value);
    }

    public bool IsWorkflowBusy
    {
        get => _isWorkflowBusy;
        private set => SetProperty(ref _isWorkflowBusy, value);
    }

    public string WorkflowStatus
    {
        get => _workflowStatus;
        private set => SetProperty(ref _workflowStatus, value);
    }

    public string ActiveWorkflowLabel
    {
        get => _activeWorkflowLabel;
        private set => SetProperty(ref _activeWorkflowLabel, value);
    }

    public string LastWorkflowCommand
    {
        get => _lastWorkflowCommand;
        private set => SetProperty(ref _lastWorkflowCommand, value);
    }

    public string DatasetNarrative
    {
        get => _datasetNarrative;
        private set => SetProperty(ref _datasetNarrative, value);
    }

    public string BootstrapNarrative
    {
        get => _bootstrapNarrative;
        private set => SetProperty(ref _bootstrapNarrative, value);
    }

    public string RefreshPlanNarrative
    {
        get => _refreshPlanNarrative;
        private set => SetProperty(ref _refreshPlanNarrative, value);
    }

    public string GraphPackageNarrative
    {
        get => _graphPackageNarrative;
        private set => SetProperty(ref _graphPackageNarrative, value);
    }

    public string SplitNarrative
    {
        get => _splitNarrative;
        private set => SetProperty(ref _splitNarrative, value);
    }

    public string GraphDesignNarrative
    {
        get => _graphDesignNarrative;
        private set => SetProperty(ref _graphDesignNarrative, value);
    }

    public string SelectedModelHeadline
    {
        get => _selectedModelHeadline;
        private set => SetProperty(ref _selectedModelHeadline, value);
    }

    public string SelectedModelPitch
    {
        get => _selectedModelPitch;
        private set => SetProperty(ref _selectedModelPitch, value);
    }

    public string RunSummary
    {
        get => _runSummary;
        private set => SetProperty(ref _runSummary, value);
    }

    public string SelectedRunName
    {
        get => _selectedRunName;
        private set => SetProperty(ref _selectedRunName, value);
    }

    public string InferenceNarrative
    {
        get => _inferenceNarrative;
        private set => SetProperty(ref _inferenceNarrative, value);
    }

    public string TrainingExecutionNarrative
    {
        get => _trainingExecutionNarrative;
        private set => SetProperty(ref _trainingExecutionNarrative, value);
    }

    public string EnvironmentGuidance
    {
        get => _environmentGuidance;
        private set => SetProperty(ref _environmentGuidance, value);
    }

    public string EnvironmentFixCommands
    {
        get => _environmentFixCommands;
        private set => SetProperty(ref _environmentFixCommands, value);
    }

    public PointCollection TrainingCurvePoints
    {
        get => _trainingCurvePoints;
        private set => SetProperty(ref _trainingCurvePoints, value);
    }

    public PointCollection ValidationCurvePoints
    {
        get => _validationCurvePoints;
        private set => SetProperty(ref _validationCurvePoints, value);
    }

    [RelayCommand]
    private async Task StartGuidedDemo()
    {
        await RunWorkspaceCommandAsync("Initialize workspace", "setup-workspace");
        LoadWorkspaceSnapshot();
        AppendLog("Workflow", "Workspace manifest refreshed and the guided review is ready.");
        AdvanceToStep(1);
    }

    [RelayCommand]
    private async Task RefreshWorkspace()
    {
        await RunWorkspaceCommandAsync("Refresh workspace status", "status");
        LoadWorkspaceSnapshot();
        AppendLog("Workspace", $"Refreshed workspace view from {WorkspaceRootInput}.");
    }

    [RelayCommand]
    private async Task ApplyWorkspaceRoot()
    {
        WorkspaceRootInput = string.IsNullOrWhiteSpace(WorkspaceRootInput)
            ? _workspaceDataService.DetectWorkspaceRoot()
            : Path.GetFullPath(WorkspaceRootInput.Trim());
        await RunWorkspaceCommandAsync("Check workspace readiness", "doctor");
        LoadWorkspaceSnapshot();
        AppendLog("Workspace", $"Switched workspace root to {WorkspaceRootInput}.");
    }

    [RelayCommand]
    private async Task ExecuteCurrentStepAction()
    {
        switch (_currentStepIndex)
        {
            case 0:
                await StartGuidedDemo();
                break;
            case 1:
                await BuildBootstrapStore();
                break;
            case 2:
                await PreviewSearch();
                break;
            case 3:
                await AssembleDataset();
                break;
            case 4:
                await DesignSplit();
                break;
            case 5:
                await PlanSelectedPdbRefresh();
                break;
            case 6:
                await RecommendModel();
                break;
            case 7:
                await TrainModel();
                break;
            default:
                await RunInference();
                break;
        }
    }

    [RelayCommand]
    private async Task RunCompleteStory()
    {
        await StartGuidedDemo();
        await BuildBootstrapStore();
        await PreviewSearch();
        await AssembleDataset();
        await DesignSplit();
        await PlanSelectedPdbRefresh();
        await RecommendModel();
        await TrainModel();
        await CompareRuns();
        await RunInference();
        await RunWorkspaceCommandAsync("Export workspace snapshot", "export-demo-snapshot");
    }

    [RelayCommand]
    private void ResetDemo()
    {
        _currentStepIndex = 0;
        _completedStepCount = 0;
        _selectedDatasetProfileKey = "broad";
        _selectedBalanceKey = "family";
        _selectedEmbeddingKey = "esm2";
        _selectedObjectiveKey = "hybrid";
        _selectedModelFamilyKey = "hybrid";
        _selectedModalityKey = "tri_modal";
        _selectedSpeedKey = "balanced";
        _selectedTrainingExecutionModeKey = "auto";
        _selectedInferenceScenarioKey = "novel_pocket";
        _selectedGraphLevelKey = "residue";
        _selectedGraphScopeKey = "interface_only";
        _selectedGraphExportKey = "training";
        _selectedGraphTargetKey = "refresh_plan";

        OnPropertyChanged(nameof(SelectedDatasetProfileKey));
        OnPropertyChanged(nameof(SelectedBalanceKey));
        OnPropertyChanged(nameof(SelectedEmbeddingKey));
        OnPropertyChanged(nameof(SelectedObjectiveKey));
        OnPropertyChanged(nameof(SelectedModelFamilyKey));
        OnPropertyChanged(nameof(SelectedModalityKey));
        OnPropertyChanged(nameof(SelectedSpeedKey));
        OnPropertyChanged(nameof(SelectedTrainingExecutionModeKey));
        OnPropertyChanged(nameof(SelectedInferenceScenarioKey));
        OnPropertyChanged(nameof(SelectedGraphLevelKey));
        OnPropertyChanged(nameof(SelectedGraphScopeKey));
        OnPropertyChanged(nameof(SelectedGraphExportKey));
        OnPropertyChanged(nameof(SelectedGraphTargetKey));

        ActivityLog.Clear();
        WorkflowConsoleLines.Clear();
        WorkflowStatus = "Idle";
        ActiveWorkflowLabel = "No command running";
        LastWorkflowCommand = "No command run yet";
        LoadWorkspaceSnapshot();
    }

    [RelayCommand]
    private async Task PreviewSearch()
    {
        await RunWorkspaceCommandAsync("Preview representative search", "preview-rcsb-search");
        LoadWorkspaceSnapshot();
        AppendLog("Search", "Representative search expanded across protein-protein, protein-ligand, and single-protein control strata.");
        AdvanceToStep(3);
    }

    [RelayCommand]
    private async Task BuildBootstrapStore()
    {
        await RunWorkspaceCommandAsync("Build local bootstrap store", "materialize-bootstrap-store");
        LoadWorkspaceSnapshot();
        AppendLog("Bootstrap", $"Indexed a local-first bootstrap store with {BootstrapStats.FirstOrDefault(card => card.Label == "Indexed PDBs")?.Value ?? "0"} PDB records for fast planning.");
        AdvanceToStep(2);
        RebuildArtifacts("Bootstrap store prepared");
    }

    [RelayCommand]
    private async Task AssembleDataset()
    {
        await RunWorkspaceCommandAsync("Build custom training set", "build-custom-training-set");
        LoadWorkspaceSnapshot();
        RefreshDatasetProjection();
        AppendLog("Dataset", $"Built a graph-ready {Lookup(DatasetProfiles, SelectedDatasetProfileKey)} set with {DatasetStats.FirstOrDefault()?.Value ?? "tens of thousands"} paired examples.");
        AdvanceToStep(4);
        RebuildArtifacts("Dataset package prepared");
    }

    [RelayCommand]
    private async Task EngineerDataset()
    {
        await RunWorkspaceCommandAsync("Engineer model-ready dataset", "engineer-dataset");
        LoadWorkspaceSnapshot();
        RefreshDatasetProjection();
        RefreshModelProjection();
        AppendLog("Dataset", "Exported an engineered dataset with diversity, feature-schema, and graph-coverage metadata for downstream model selection.");
        RebuildArtifacts("Engineered dataset refreshed");
    }

    [RelayCommand]
    private async Task DesignSplit()
    {
        await RunWorkspaceCommandAsync("Build leakage-aware splits", "build-splits");
        LoadWorkspaceSnapshot();
        SplitNarrative = SelectedBalanceKey switch
        {
            "novelty" => "Split design now emphasizes novel folds, rare motif families, and source-held-out assay contexts so the benchmark reads as a generalization test rather than memorization.",
            "assay" => "Split design now equalizes assay regimes and source provenance to prevent one measurement family from dominating the evaluation story.",
            _ => "Split design now groups by family, motif, and source so near-duplicate proteins and mutation clusters stay on one side of the evaluation boundary.",
        };
        AppendLog("Split", "Leakage-resistant split designed with family, motif, and source-aware grouping.");
        AdvanceToStep(5);
        RebuildArtifacts("Split manifest prepared");
    }

    [RelayCommand]
    private async Task PlanSelectedPdbRefresh()
    {
        await RunWorkspaceCommandAsync("Plan selected-PDB refresh", "plan-selected-pdb-refresh");
        LoadWorkspaceSnapshot();
        AppendLog("Refresh", $"Scoped a targeted refresh plan for {RefreshPlanStats.FirstOrDefault(card => card.Label == "Selected PDBs")?.Value ?? "0"} selected PDB IDs.");
        AdvanceToStep(6);
        RebuildArtifacts("Targeted refresh plan prepared");
    }

    [RelayCommand]
    private async Task RunSelectedPdbRefresh()
    {
        await RunWorkspaceCommandAsync("Refresh selected PDB assets", "refresh-selected-pdbs");
        LoadWorkspaceSnapshot();
        AppendLog("Refresh", "Updated the selected-PDB asset set using the current targeted refresh manifest.");
        RebuildArtifacts("Selected-PDB refresh executed");
    }

    [RelayCommand]
    private async Task BuildStructuralGraphs()
    {
        var args = new List<string>
        {
            "build-structural-graphs",
            "--graph-level", SelectedGraphLevelKey,
            "--scope", SelectedGraphScopeKey,
            "--shell-radius", SelectedGraphScopeKey == "shell" ? "8.0" : "6.0",
            "--selection", SelectedGraphTargetKey,
        };

        if (SelectedGraphTargetKey == "preview")
        {
            args.Add("--limit");
            args.Add("8");
        }

        foreach (var exportFormat in ResolveGraphExportFormats())
        {
            args.Add("--export-format");
            args.Add(exportFormat);
        }

        await RunWorkspaceCommandAsync("Build structural graphs", args.ToArray());
        LoadWorkspaceSnapshot();
        RefreshGraphProjection();
        AppendLog("Graphs", $"Built {Lookup(GraphLevels, SelectedGraphLevelKey)} artifacts with {Lookup(GraphScopes, SelectedGraphScopeKey)} scope for {Lookup(GraphBuildTargets, SelectedGraphTargetKey)} using {Lookup(GraphExportBundles, SelectedGraphExportKey)} exports.");
        RebuildArtifacts("Structural graph package prepared");
    }

    [RelayCommand]
    private async Task RefreshSelectedAndBuildGraphs()
    {
        if (!RefreshPlanStats.Any(card => card.Label == "Refresh plan" && card.Value.Equals("Ready", StringComparison.OrdinalIgnoreCase)))
        {
            await PlanSelectedPdbRefresh();
        }

        SelectedGraphTargetKey = "refresh_plan";
        await RunSelectedPdbRefresh();
        await BuildStructuralGraphs();
    }

    [RelayCommand]
    private async Task RecommendModel()
    {
        if (!HasReadyGraphPackage())
        {
            await BuildStructuralGraphs();
        }

        await EngineerDataset();

        if (SelectedModelFamilyKey == "auto")
        {
            SelectedModelFamilyKey = DetermineRecommendedModelFamily();
        }

        await RunWorkspaceCommandAsync("Report training quality", "report-training-set-quality");
        await RunWorkspaceCommandAsync("Export model recommendation", "report-model-recommendation");
        LoadWorkspaceSnapshot();
        RefreshModelProjection();
        AppendLog("Model Studio", $"Recommended {Lookup(ModelFamilies, SelectedModelFamilyKey)} based on the selected objective plus current graph coverage.");
        AdvanceToStep(7);
    }

    [RelayCommand]
    private async Task TrainModel()
    {
        var graphCoverage = EstimateGraphCoverage();
        if ((SelectedModelFamilyKey == "gnn" || SelectedModelFamilyKey == "hybrid") && graphCoverage < 0.40)
        {
            AppendLog("Training", "Graph coverage is currently light for a graph-heavy training path. A stronger tabular baseline may generalize more cleanly until graph coverage improves.");
        }

        await RunWorkspaceCommandAsync("Train recommended model", "train-recommended-model", "--execution-strategy", SelectedTrainingExecutionModeKey);
        LoadWorkspaceSnapshot();
        RefreshModelProjection();
        SelectedRunName = $"{Lookup(ModelFamilies, SelectedModelFamilyKey)} - recommended path";
        var baseSummary = SelectedModelFamilyKey switch
        {
            "rf" => "Fast, interpretable baseline with clear feature attributions and a slightly lower ceiling on novel-family generalization.",
            "xgb" => "Boosted tabular learner that improves ranking quality and calibration while keeping the workflow lightweight.",
            "gnn" => "Graph-heavy architecture emphasizing residue neighborhoods and contact topology for interface-rich tasks.",
            _ => "Fusion model that blends structure graphs, embeddings, and assay descriptors to produce the strongest overall platform performance.",
        };
        RunSummary = (SelectedModelFamilyKey == "gnn" || SelectedModelFamilyKey == "hybrid") && graphCoverage < 0.40
            ? $"{baseSummary} Current dataset graph coverage is still light, so this run should be treated as exploratory until graph-backed coverage improves."
            : baseSummary;

        AppendLog("Training", $"Trained {SelectedRunName} from the exported starter config and produced a saved model-studio run with metrics and artifacts.");
        AdvanceToStep(8);
        RebuildArtifacts("Training report prepared");
        RebuildComparisonRuns();
    }

    [RelayCommand]
    private async Task CompareRuns()
    {
        await RunWorkspaceCommandAsync("Generate model comparison", "report-model-comparison");
        LoadWorkspaceSnapshot();
        RebuildComparisonRuns();
        AppendLog("Comparison", "Compared the current run against alternate model families with plausible tradeoffs in speed, interpretability, and accuracy.");
        AdvanceToStep(8);
        RebuildArtifacts("Comparison report prepared");
    }

    [RelayCommand]
    private async Task RunInference()
    {
        await RunWorkspaceCommandAsync("Run ligand screening inference", "predict-ligand-screening", "--smiles", "CCO");
        LoadWorkspaceSnapshot();
        RefreshInferenceProjection();
        AppendLog("Inference", $"Ran saved-model inference for the {Lookup(InferenceScenarios, SelectedInferenceScenarioKey)} scenario.");
        AdvanceToStep(_steps.Count);
        RebuildArtifacts("Inference packet prepared");
    }

    [RelayCommand]
    private async Task RunStatusCheck()
    {
        await RunWorkspaceCommandAsync("Run workspace status check", "status");
        LoadWorkspaceSnapshot();
    }

    [RelayCommand]
    private async Task RunDoctorCheck()
    {
        await RunWorkspaceCommandAsync("Run environment check", "doctor");
        LoadWorkspaceSnapshot();
    }

    [RelayCommand]
    private async Task RunSmokeCheck()
    {
        if (IsWorkflowBusy)
        {
            AppendLog("Workflow", "Skipped smoke check because another command is still running.");
            return;
        }

        IsWorkflowBusy = true;
        ActiveWorkflowLabel = "Run repo smoke check";
        LastWorkflowCommand = "python scripts/run_repo_smoke.py --quick";
        WorkflowStatus = "Running";
        WorkflowConsoleLines.Clear();
        AddWorkflowConsoleLine($"> {LastWorkflowCommand}");
        AppendLog("Workflow", "Starting repo smoke check.");

        try
        {
            var result = await _workflowCommandService.RunScriptAsync(
                WorkspaceRootInput,
                Path.Combine("scripts", "run_repo_smoke.py"),
                new[] { "--quick" },
                line => _dispatcherQueue.TryEnqueue(() => AddWorkflowConsoleLine(line)));

            WorkflowStatus = result.Succeeded ? "Completed" : $"Failed ({result.ExitCode})";
            AddWorkflowConsoleLine(result.Succeeded
                ? "Smoke check completed successfully."
                : $"Smoke check failed with exit code {result.ExitCode}.");
            AppendLog("Workflow", result.Succeeded
                ? "Repo smoke check completed."
                : $"Repo smoke check failed with exit code {result.ExitCode}.");
            LoadWorkspaceSnapshot();
        }
        catch (Exception ex)
        {
            WorkflowStatus = "Failed";
            AddWorkflowConsoleLine($"Smoke runner failed: {ex.Message}");
            AppendLog("Workflow", $"Repo smoke check failed before completion: {ex.Message}");
        }
        finally
        {
            IsWorkflowBusy = false;
            ActiveWorkflowLabel = "No command running";
        }
    }

    private void AdvanceToStep(int completedSteps)
    {
        _completedStepCount = Math.Clamp(completedSteps, 0, _steps.Count);
        _currentStepIndex = Math.Min(_completedStepCount, _steps.Count - 1);
        RebuildTimeline();
        NotifyGuideState();
    }

    private void NotifyGuideState()
    {
        OnPropertyChanged(nameof(CurrentStepTitle));
        OnPropertyChanged(nameof(CurrentStepWhy));
        OnPropertyChanged(nameof(CurrentStepClick));
        OnPropertyChanged(nameof(CurrentStepFind));
        OnPropertyChanged(nameof(CurrentRecommendedPageKey));
        OnPropertyChanged(nameof(CurrentActionLabel));
        OnPropertyChanged(nameof(ProgressLabel));
    }

    private bool HasReadyGraphPackage()
    {
        return GraphPackageStats.Any(card =>
            card.Label.Equals("Graph package", StringComparison.OrdinalIgnoreCase)
            && card.Value.Equals("Ready", StringComparison.OrdinalIgnoreCase));
    }

    private void RefreshDatasetProjection()
    {
        RefreshGraphProjection();
        if (_workspaceStageLookup.Count > 0)
        {
            return;
        }
        DatasetStats.Clear();

        var profile = SelectedDatasetProfileKey switch
        {
            "ppi" => (candidates: 84210, paired: 18620, motifs: 540, holdouts: 172, leakage: "Very low", note: "Protein-protein assemblies dominate, with multimer interfaces and mutation clusters grouped tightly."),
            "ligand" => (candidates: 129430, paired: 24110, motifs: 690, holdouts: 201, leakage: "Low", note: "Ligand-pocket diversity rises, with chemotype balancing and pocket-family holdout emphasized."),
            "single" => (candidates: 63540, paired: 12180, motifs: 410, holdouts: 149, leakage: "Very low", note: "Controls emphasize background structural context, negative examples, and single-chain calibration cases."),
            _ => (candidates: 157860, paired: 28340, motifs: 760, holdouts: 214, leakage: "Low", note: "A broad, representative mix of structural complexes, ligands, and control contexts remains in play."),
        };

        var embeddingBoost = SelectedEmbeddingKey switch
        {
            "prott5" => 1.08,
            "esm2" => 1.04,
            _ => 0.96,
        };

        var balanceText = SelectedBalanceKey switch
        {
            "novelty" => "Rare folds and under-sampled motifs receive a deliberate boost in candidate retention.",
            "assay" => "Assay regimes are flattened so one measurement family does not dominate the training story.",
            _ => "Family, fold, and sequence identity balancing remain the dominant constraint.",
        };

        DatasetStats.Add(new() { Label = "Candidate structures", Value = $"{profile.candidates:N0}", Caption = "Representative structures that match the selected preview scope." });
        DatasetStats.Add(new() { Label = "Paired examples", Value = $"{profile.paired:N0}", Caption = "Graph-ready training examples after source reconciliation and filtering." });
        DatasetStats.Add(new() { Label = "Motif groups", Value = $"{(int)Math.Round(profile.motifs * embeddingBoost):N0}", Caption = "Grouped motifs, domains, and structural themes used for balancing." });
        DatasetStats.Add(new() { Label = "Held-out families", Value = $"{profile.holdouts:N0}", Caption = "Protein families reserved to make evaluation feel genuinely out-of-family." });

        DatasetNarrative = $"{profile.note} {balanceText} Embedding path: {Lookup(EmbeddingStrategies, SelectedEmbeddingKey)}. Graph design: {Lookup(GraphLevels, SelectedGraphLevelKey)} with {Lookup(GraphScopes, SelectedGraphScopeKey)} scope.";
        SplitNarrative = $"Current leakage risk reads as {profile.leakage}. The split logic keeps sequence-near duplicates, fold relatives, and source clusters from leaking across train and evaluation.";
    }

    private void RefreshModelProjection()
    {
        if (_workspaceStageLookup.Count > 0)
        {
            return;
        }
        var family = SelectedModelFamilyKey;
        var objective = SelectedObjectiveKey;
        var modality = SelectedModalityKey;
        var speed = SelectedSpeedKey;
        var embedding = SelectedEmbeddingKey;
        var graphLevel = SelectedGraphLevelKey;
        var graphScope = SelectedGraphScopeKey;
        var graphCoverage = EstimateGraphCoverage();

        var baseAuprc = family switch
        {
            "rf" => 0.71,
            "xgb" => 0.76,
            "gnn" => 0.81,
            "auto" => 0.79,
            _ => 0.84,
        };

        var baseAuroc = family switch
        {
            "rf" => 0.83,
            "xgb" => 0.87,
            "gnn" => 0.90,
            "auto" => 0.89,
            _ => 0.92,
        };

        if (embedding == "prott5" && (family == "hybrid" || modality == "tri_modal"))
        {
            baseAuprc += 0.01;
        }
        else if (embedding == "none")
        {
            baseAuprc -= 0.02;
            baseAuroc -= 0.01;
        }

        if (speed == "fast")
        {
            baseAuprc -= 0.02;
            baseAuroc -= 0.015;
        }
        else if (speed == "best")
        {
            baseAuprc += 0.015;
            baseAuroc += 0.01;
        }

        if (objective == "interface" && family == "gnn")
        {
            baseAuprc += 0.015;
        }
        else if (objective == "screen" && family == "xgb")
        {
            baseAuprc += 0.01;
        }

        if (family == "gnn" || family == "hybrid")
        {
            if (graphCoverage >= 0.8)
            {
                baseAuprc += 0.012;
                baseAuroc += 0.008;
            }
            else if (graphCoverage < 0.4)
            {
                baseAuprc -= 0.03;
                baseAuroc -= 0.02;
            }
        }
        else if ((family == "rf" || family == "xgb") && graphCoverage < 0.4)
        {
            baseAuprc += 0.008;
        }

        SelectedModelHeadline = family switch
        {
            "rf" => "Tree ensemble baseline with compact, interpretable feature flows",
            "xgb" => "Boosted tabular learner tuned for ranking-heavy binding screens",
            "gnn" => "Residue-contact graph model emphasizing topology and neighborhood signal",
            "auto" => "Automatically chosen path balancing speed, interpretability, and modality fit",
            _ => "Hybrid fusion model combining graph, descriptor, and embedding branches",
        };

        var graphCoverageText = graphCoverage switch
        {
            >= 0.8 => "Graph coverage is strong enough to support graph-heavy learning.",
            >= 0.4 => "Graph coverage is moderate, so a balanced hybrid path is safer than a purely graph-first story.",
            _ => "Graph coverage is still light, so tabular-heavy training may generalize more reliably right now."
        };
        SelectedModelPitch = $"Objective: {Lookup(Objectives, SelectedObjectiveKey)}. Modality blend: {Lookup(Modalities, SelectedModalityKey)}. Embedding strategy: {Lookup(EmbeddingStrategies, SelectedEmbeddingKey)}. Graph path: {Lookup(GraphLevels, graphLevel)} with {Lookup(GraphScopes, graphScope)} scope. {graphCoverageText}";

        TrainingStats.Clear();
        TrainingStats.Add(new() { Label = "Validation AUPRC", Value = baseAuprc.ToString("0.000"), Caption = "Useful for imbalanced binding prediction and ranking." });
        TrainingStats.Add(new() { Label = "Validation AUROC", Value = baseAuroc.ToString("0.000"), Caption = "General separation power across held-out families." });
        TrainingStats.Add(new() { Label = "Calibration", Value = (0.79 + (baseAuprc - 0.70) * 0.65).ToString("0.000"), Caption = "Confidence quality after the preview calibration step." });
        TrainingStats.Add(new() { Label = "Train time", Value = speed switch { "fast" => "3m 12s", "best" => "11m 48s", _ => "6m 24s" }, Caption = "Shortened for preview mode while preserving realistic differences." });

        MetricBars.Clear();
        MetricBars.Add(new() { Label = "Novel-family generalization", Value = Clamp(baseAuprc + 0.04), DisplayValue = Clamp(baseAuprc + 0.04).ToString("0.000") });
        MetricBars.Add(new() { Label = "Pocket transferability", Value = Clamp(baseAuprc - 0.01), DisplayValue = Clamp(baseAuprc - 0.01).ToString("0.000") });
        MetricBars.Add(new() { Label = "Interpretability", Value = family switch { "rf" => 0.90, "xgb" => 0.82, "gnn" => 0.70, _ => 0.78 }, DisplayValue = family switch { "rf" => "high", "xgb" => "med-high", "gnn" => "medium", _ => "medium" } });
        MetricBars.Add(new() { Label = "Inference throughput", Value = speed switch { "fast" => 0.92, "best" => 0.74, _ => 0.83 }, DisplayValue = speed switch { "fast" => "very fast", "best" => "slower", _ => "balanced" } });
        RefreshTrainingExecutionNarrative();

        ArchitectureStages.Clear();
        AddArchitectureStage("1", family switch { "gnn" => $"{Lookup(GraphLevels, graphLevel)} intake", "rf" or "xgb" => "Engineered descriptor table", _ => "Graph + descriptor intake" }, $"Graph scope: {Lookup(GraphScopes, graphScope)}. The app reconciles structure, motif, assay, and embedding context into one coherent input layer.", DemoPalette.Aqua);
        AddArchitectureStage("2", embedding switch { "prott5" => "ProtT5 sequence context", "esm2" => "ESM2 embedding context", _ => "No embedding branch" }, "Sequence representation either complements structure or intentionally stays off for a baseline comparison.", DemoPalette.Blue);
        AddArchitectureStage("3", family switch { "rf" => "Tree ensemble scoring", "xgb" => "Boosted ranking blocks", "gnn" => "Message-passing layers", _ => "Fusion and gating block" }, "This is the main scoring engine that turns aligned evidence into a binding prediction.", DemoPalette.Gold);
        AddArchitectureStage("4", objective switch { "interface" => "Interface classification head", "screen" => "Pocket ranking head", _ => "Calibrated binding head" }, "The final head changes with the biological question so the output looks task-aware rather than generic.", DemoPalette.Coral);

        var trainLoss = family switch
        {
            "rf" => new double[] { 0.78, 0.69, 0.62, 0.58, 0.56, 0.54, 0.53, 0.53 },
            "xgb" => new double[] { 0.76, 0.66, 0.59, 0.54, 0.51, 0.49, 0.48, 0.47 },
            "gnn" => new double[] { 0.82, 0.68, 0.57, 0.49, 0.44, 0.41, 0.39, 0.38 },
            _ => new double[] { 0.85, 0.70, 0.58, 0.49, 0.43, 0.39, 0.36, 0.34 },
        };
        var validation = family switch
        {
            "rf" => new double[] { 0.80, 0.74, 0.68, 0.64, 0.61, 0.59, 0.58, 0.58 },
            "xgb" => new double[] { 0.78, 0.71, 0.64, 0.58, 0.55, 0.53, 0.52, 0.51 },
            "gnn" => new double[] { 0.84, 0.73, 0.63, 0.55, 0.50, 0.47, 0.45, 0.44 },
            _ => new double[] { 0.86, 0.75, 0.64, 0.55, 0.49, 0.45, 0.42, 0.40 },
        };
        TrainingCurvePoints = BuildCurve(trainLoss);
        ValidationCurvePoints = BuildCurve(validation);
    }

    private string DetermineRecommendedModelFamily()
    {
        var graphCoverage = EstimateGraphCoverage();
        if (graphCoverage >= 0.8)
        {
            return SelectedObjectiveKey == "screen" ? "hybrid" : "gnn";
        }

        if (graphCoverage >= 0.4)
        {
            return SelectedObjectiveKey == "screen" ? "xgb" : "hybrid";
        }

        return SelectedObjectiveKey switch
        {
            "screen" => "xgb",
            "interface" => "xgb",
            _ => "rf",
        };
    }

    private double EstimateGraphCoverage()
    {
        var coverageCard = TrainingStats.FirstOrDefault(card => card.Label.Equals("Graph coverage", StringComparison.OrdinalIgnoreCase));
        if (coverageCard is not null)
        {
            var text = coverageCard.Value.Replace("%", string.Empty).Trim();
            if (double.TryParse(text, out var percent))
            {
                return percent > 1.0 ? percent / 100.0 : percent;
            }
        }

        var graphEntriesCard = GraphPackageStats.FirstOrDefault(card => card.Label.Equals("Graph entries", StringComparison.OrdinalIgnoreCase));
        if (graphEntriesCard is not null && int.TryParse(graphEntriesCard.Value.Replace(",", string.Empty), out var graphEntries) && graphEntries > 0)
        {
            return graphEntries >= 100 ? 0.8 : graphEntries >= 25 ? 0.5 : 0.25;
        }

        return 0.0;
    }

    private void RefreshInferenceProjection()
    {
        if (_workspaceStageLookup.Count > 0)
        {
            return;
        }
        Predictions.Clear();

        if (SelectedInferenceScenarioKey == "ppi_triage")
        {
            Predictions.Add(new() { PairLabel = "HSP90 / CDC37 interface", Score = "0.912", Confidence = "High", Rationale = "Strong contact density, compatible motif neighborhoods, and family-consistent interface geometry.", RiskNote = "Watch for scaffold over-representation in chaperone-rich folds." });
            Predictions.Add(new() { PairLabel = "PD-1 / PD-L1 variant", Score = "0.873", Confidence = "High", Rationale = "Interface residues align well with learned hotspot neighborhoods and calibration remains stable.", RiskNote = "Mutation cluster is close to seen immune checkpoint families." });
            Predictions.Add(new() { PairLabel = "RAS / RAF pocketed assembly", Score = "0.801", Confidence = "Medium", Rationale = "Good graph topology match, but assay context is thinner than the top-ranked examples.", RiskNote = "Sparser assay support than the top two predictions." });
            InferenceNarrative = "This inference view highlights an interface-heavy triage story, where the saved model ranks protein-protein complexes and surfaces likely reasons for strong interaction confidence.";
        }
        else if (SelectedInferenceScenarioKey == "mutant_scan")
        {
            Predictions.Add(new() { PairLabel = "EGFR L858R pocket", Score = "0.884", Confidence = "High", Rationale = "Mutation-aware motif context and sequence embedding shifts both support retained binding behavior.", RiskNote = "Calibration drops slightly on rare mutant families." });
            Predictions.Add(new() { PairLabel = "BRAF V600E cleft", Score = "0.842", Confidence = "High", Rationale = "Graph and embedding branches agree on a motif-preserving mutation pattern.", RiskNote = "Assay transfer is moderate rather than broad." });
            Predictions.Add(new() { PairLabel = "KRAS G12D site", Score = "0.761", Confidence = "Medium", Rationale = "Signal stays positive, but the model sees more structural uncertainty in the altered loop.", RiskNote = "Conformation uncertainty is the main caveat." });
            InferenceNarrative = "Inference now reads like a mutation sensitivity pass, where the platform surfaces how motif-preserving and motif-disrupting residue changes alter score confidence.";
        }
        else
        {
            Predictions.Add(new() { PairLabel = "MCL1 hydrophobic cleft", Score = "0.921", Confidence = "High", Rationale = "Pocket geometry, motif coverage, and embedding context all align with strong binder patterns.", RiskNote = "Minimal; chemistry is well represented in the current training coverage." });
            Predictions.Add(new() { PairLabel = "BRD4 bromodomain pocket", Score = "0.866", Confidence = "High", Rationale = "Assay evidence and structural motif placement both support ranking this near the top.", RiskNote = "Generalization remains strongest for bromodomain-adjacent families." });
            Predictions.Add(new() { PairLabel = "CDK2 allosteric niche", Score = "0.793", Confidence = "Medium", Rationale = "The graph branch is confident, though assay context is sparser than the top hits.", RiskNote = "Pocket novelty is higher here, so the explanation calls out moderate uncertainty." });
            InferenceNarrative = "This inference view highlights a novel pocket screening story, where the saved model surfaces ranked hits plus the motifs and structural reasons behind each score.";
        }
    }

    private void RefreshGraphProjection()
    {
        var graphLevelLabel = Lookup(GraphLevels, SelectedGraphLevelKey);
        var graphScopeLabel = Lookup(GraphScopes, SelectedGraphScopeKey);
        var exportLabel = Lookup(GraphExportBundles, SelectedGraphExportKey);
        var targetLabel = Lookup(GraphBuildTargets, SelectedGraphTargetKey);

        GraphDesignStats.Clear();
        GraphDesignStats.Add(new()
        {
            Label = "Graph level",
            Value = graphLevelLabel,
            Caption = SelectedGraphLevelKey == "atom"
                ? "Atom-level graphs preserve chemistry, bond heuristics, and coordination detail."
                : "Residue-level graphs compress contact topology for larger-scale training and screening."
        });
        GraphDesignStats.Add(new()
        {
            Label = "Graph scope",
            Value = graphScopeLabel,
            Caption = SelectedGraphScopeKey switch
            {
                "whole_protein" => "Retain global structural context across the entire complex.",
                "shell" => "Focus a local neighborhood around interfaces or pockets while keeping nearby context.",
                _ => "Limit the graph to the active interaction surface and hotspot residues."
            }
        });
        GraphDesignStats.Add(new()
        {
            Label = "Export bundle",
            Value = exportLabel,
            Caption = SelectedGraphExportKey switch
            {
                "interop" => "Keep PyG, DGL, and NetworkX outputs available for the full experimentation surface.",
                "audit" => "Produce lighter review-friendly graph exports without training-specific tensor bundles.",
                _ => "Expose the main training path while preserving a readable inspection format."
            }
        });
        GraphDesignStats.Add(new()
        {
            Label = "Build target",
            Value = targetLabel,
            Caption = SelectedGraphTargetKey switch
            {
                "preview" => "Uses a smaller proof slice first so a new clone can validate graph packaging quickly.",
                "training_set" => "Follows the active curated training CSV instead of the full workspace.",
                "all" => "Targets every currently extracted local structure, which is the heaviest but broadest packaging pass.",
                _ => "Follows the targeted selected-PDB refresh plan so graph work stays aligned with the chosen active set."
            }
        });
        GraphDesignStats.Add(new()
        {
            Label = "Design space",
            Value = "Full set",
            Caption = "Whole-protein, interface-only, shell, residue-level, atom-level, and multi-export graph variants remain available."
        });
        if (GraphPackageStats.Count > 0)
        {
            foreach (var card in GraphPackageStats)
            {
                GraphDesignStats.Add(card);
            }
        }

        GraphDesignNarrative = SelectedGraphScopeKey switch
        {
            "whole_protein" => $"{graphLevelLabel} design keeps the entire complex in view, which is useful when long-range topology or distal allostery may matter.",
            "shell" => $"{graphLevelLabel} design uses a shell around the active site so we keep local geometry without paying the full whole-structure cost.",
            _ => $"{graphLevelLabel} design concentrates directly on the interface or pocket, which is ideal when the training question is driven by contact geometry."
        } + $" Graph packaging currently targets {targetLabel}, and exports land as {exportLabel}. {GraphPackageNarrative}";
    }

    private void RebuildComparisonRuns()
    {
        if (_workspaceStageLookup.Count > 0 && ComparisonRuns.Count > 0)
        {
            return;
        }
        ComparisonRuns.Clear();

        ComparisonRuns.Add(new() { Name = "Hybrid Fusion - novelty holdout", Family = "Hybrid Fusion", HeadlineMetric = "AUPRC 0.846", SupportMetric = "AUROC 0.921", Note = "Best overall transfer in the current preview run set.", AccentBrush = DemoPalette.Aqua });
        ComparisonRuns.Add(new() { Name = "Graph Neural Net - interface heavy", Family = "Graph Neural Net", HeadlineMetric = "AUPRC 0.811", SupportMetric = "AUROC 0.901", Note = "Excellent interface reasoning, slightly weaker calibration than fusion.", AccentBrush = DemoPalette.Blue });
        ComparisonRuns.Add(new() { Name = "XGBoost - descriptor baseline", Family = "XGBoost", HeadlineMetric = "AUPRC 0.762", SupportMetric = "AUROC 0.871", Note = "Useful fast baseline with strong interpretability and lower overhead.", AccentBrush = DemoPalette.Gold });
    }

    private void RebuildArtifacts(string status)
    {
        if (_workspaceStageLookup.Count > 0 && ArtifactSummaries.Count > 0 && status != "Ready for preview")
        {
            return;
        }
        ArtifactSummaries.Clear();
        ArtifactSummaries.Add(new() { Title = "Representative search report", Status = status, Summary = "Search breadth, source mix, and representative clustering overview." });
        ArtifactSummaries.Add(new() { Title = "Balanced training package", Status = status, Summary = "Leakage-aware split summary, motif coverage, and family holdout notes." });
        ArtifactSummaries.Add(new() { Title = "Model Studio run bundle", Status = status, Summary = "Training curves, metric scorecard, architecture notes, and comparison report." });
        ArtifactSummaries.Add(new() { Title = "Inference packet", Status = status, Summary = "Ranked hits, explanation notes, and deployment-style summary for saved-model inference." });
    }

    private void RebuildTimeline()
    {
        TimelineStages.Clear();

        for (var i = 0; i < _steps.Count; i++)
        {
            var step = _steps[i];
            var workspaceStage = ResolveWorkspaceStage(step.PageKey, step.Title);
            var statusLabel = workspaceStage?.Status switch
            {
                "completed" => "Completed",
                "completed_with_failures" => "Completed with warnings",
                "failed" => "Needs attention",
                "running" => "Running",
                _ => i < _completedStepCount ? "Completed" : i == _currentStepIndex ? "Recommended now" : "Queued",
            };
            var brush = statusLabel switch
            {
                "Completed" => DemoPalette.Aqua,
                "Completed with warnings" => DemoPalette.Coral,
                "Needs attention" => DemoPalette.Coral,
                "Running" => DemoPalette.Blue,
                _ => i == _currentStepIndex ? DemoPalette.Gold : DemoPalette.Slate,
            };

            TimelineStages.Add(new()
            {
                Title = step.Title,
                Summary = workspaceStage?.Note ?? step.WhyItMatters,
                StatusLabel = statusLabel,
                PageKey = step.PageKey,
                ActionLabel = step.ActionLabel,
                AccentBrush = brush,
            });
        }
    }

    private WorkspaceStageInfo? ResolveWorkspaceStage(string pageKey, string title)
    {
        if (_workspaceStageLookup.Count == 0)
        {
            return null;
        }

        var stageKey = title switch
        {
            "Frame the story" => "Frame the story",
            "Build local bootstrap store" => "Build local bootstrap store",
            "Preview representative coverage" => "Preview representative coverage",
            "Assemble the graph-ready set" => "Assemble graph-ready data",
            "Design the leakage-resistant split" => "Design the split",
            "Plan selected-PDB refresh" => "Plan selected-PDB refresh",
            "Refresh the model path" => "Recommend model",
            "Train the candidate" => "Train candidate model",
            "Run saved-model inference" => "Run inference",
            _ => string.Empty,
        };

        return string.IsNullOrWhiteSpace(stageKey)
            ? null
            : _workspaceStageLookup.GetValueOrDefault(stageKey);
    }

    private void LoadWorkspaceSnapshot()
    {
        WorkspaceRootInput = string.IsNullOrWhiteSpace(WorkspaceRootInput)
            ? _workspaceDataService.DetectWorkspaceRoot()
            : Path.GetFullPath(WorkspaceRootInput.Trim());

        var snapshot = _workspaceDataService.LoadSnapshot(WorkspaceRootInput);
        _workspaceStageLookup = snapshot.StageStatuses.ToDictionary(stage => stage.StageKey, StringComparer.OrdinalIgnoreCase);

        DemoHeadline = snapshot.Headline;
        DemoDisclaimer = snapshot.Disclaimer;
        WorkspaceSummary = snapshot.Summary;
        BootstrapNarrative = snapshot.BootstrapNarrative;
        RefreshPlanNarrative = snapshot.RefreshPlanNarrative;
        GraphPackageNarrative = snapshot.GraphPackageNarrative;
        EnvironmentGuidance = snapshot.EnvironmentGuidance;
        EnvironmentFixCommands = snapshot.EnvironmentFixCommands;
        DatasetNarrative = snapshot.SourceSummary;
        SplitNarrative = snapshot.SplitSummary;
        SelectedModelHeadline = snapshot.ModelHeadline;
        SelectedModelPitch = snapshot.ModelPitch;
        RunSummary = snapshot.RunSummary;
        SelectedRunName = snapshot.SelectedRunName;
        InferenceNarrative = snapshot.InferenceNarrative;

        BootstrapStats.Clear();
        foreach (var item in snapshot.BootstrapStats)
        {
            BootstrapStats.Add(item);
        }

        RefreshPlanStats.Clear();
        foreach (var item in snapshot.RefreshPlanStats)
        {
            RefreshPlanStats.Add(item);
        }

        GraphPackageStats.Clear();
        foreach (var item in snapshot.GraphPackageStats)
        {
            GraphPackageStats.Add(item);
        }

        DatasetStats.Clear();
        foreach (var item in snapshot.DatasetStats)
        {
            DatasetStats.Add(item);
        }

        EnvironmentStats.Clear();
        foreach (var item in snapshot.EnvironmentStats)
        {
            EnvironmentStats.Add(item);
        }

        TrainingStats.Clear();
        foreach (var item in snapshot.TrainingStats)
        {
            TrainingStats.Add(item);
        }

        MetricBars.Clear();
        foreach (var item in snapshot.MetricBars)
        {
            MetricBars.Add(item);
        }

        ArchitectureStages.Clear();
        foreach (var item in snapshot.ArchitectureStages)
        {
            ArchitectureStages.Add(item);
        }

        ComparisonRuns.Clear();
        foreach (var run in snapshot.ModelRuns)
        {
            ComparisonRuns.Add(new RunSummary
            {
                Name = run.RunName,
                Family = run.Family,
                HeadlineMetric = run.HeadlineMetric,
                SupportMetric = run.SupportMetric,
                Note = run.Note,
                AccentBrush = run.Family.Contains("xgboost", StringComparison.OrdinalIgnoreCase)
                    ? DemoPalette.Gold
                    : run.Family.Contains("graph", StringComparison.OrdinalIgnoreCase)
                        ? DemoPalette.Blue
                        : DemoPalette.Aqua,
            });
        }

        ArtifactSummaries.Clear();
        foreach (var artifact in snapshot.Artifacts)
        {
            ArtifactSummaries.Add(artifact);
        }

        Predictions.Clear();
        foreach (var prediction in snapshot.Predictions)
        {
            Predictions.Add(new PredictionSummary
            {
                PairLabel = prediction.PairLabel,
                Score = prediction.Score,
                Confidence = prediction.Confidence,
                Rationale = prediction.Rationale,
                RiskNote = prediction.RiskNote,
            });
        }

        ActivityLog.Clear();
        foreach (var log in snapshot.ActivityLog)
        {
            ActivityLog.Add(log);
        }

        _completedStepCount = _steps.Count(step => IsCompletedStage(ResolveWorkspaceStage(step.PageKey, step.Title)));
        var nextPendingIndex = _steps.FindIndex(step => !IsCompletedStage(ResolveWorkspaceStage(step.PageKey, step.Title)));
        _currentStepIndex = nextPendingIndex >= 0 ? nextPendingIndex : _steps.Count - 1;

        var trainingCurve = snapshot.TrainingCurveValues.Count > 1
            ? snapshot.TrainingCurveValues
            : new[] { 0.88, 0.76, 0.68, 0.59, 0.52, 0.48, 0.44, 0.41 };
        var validationCurve = snapshot.ValidationCurveValues.Count > 1
            ? snapshot.ValidationCurveValues
            : new[] { 0.91, 0.82, 0.74, 0.66, 0.61, 0.57, 0.54, 0.52 };

        TrainingCurvePoints = BuildCurve(trainingCurve);
        ValidationCurvePoints = BuildCurve(validationCurve);

        RefreshGraphProjection();
        RebuildTimeline();
        RefreshTrainingExecutionNarrative();
        NotifyGuideState();
    }

    private void AppendLog(string source, string message)
    {
        ActivityLog.Insert(0, $"[{DateTime.Now:HH:mm}] {source}: {message}");
    }

    private async Task RunWorkspaceCommandAsync(string label, params string[] args)
    {
        if (IsWorkflowBusy)
        {
            AppendLog("Workflow", $"Skipped {label} because another command is still running.");
            return;
        }

        IsWorkflowBusy = true;
        ActiveWorkflowLabel = label;
        LastWorkflowCommand = $"pbdata {string.Join(" ", args)}";
        WorkflowStatus = "Running";
        WorkflowConsoleLines.Clear();
        AddWorkflowConsoleLine($"> {LastWorkflowCommand}");
        AppendLog("Workflow", $"Starting {label}.");

        try
        {
            var result = await _workflowCommandService.RunAsync(
                WorkspaceRootInput,
                args,
                line => _dispatcherQueue.TryEnqueue(() => AddWorkflowConsoleLine(line)));

            WorkflowStatus = result.Succeeded ? "Completed" : $"Failed ({result.ExitCode})";
            AddWorkflowConsoleLine(result.Succeeded
                ? "Command completed successfully."
                : $"Command failed with exit code {result.ExitCode}.");
            AppendLog("Workflow", result.Succeeded
                ? $"{label} completed."
                : $"{label} failed with exit code {result.ExitCode}.");
        }
        catch (Exception ex)
        {
            WorkflowStatus = "Failed";
            AddWorkflowConsoleLine($"Command runner failed: {ex.Message}");
            AppendLog("Workflow", $"{label} failed before completion: {ex.Message}");
        }
        finally
        {
            IsWorkflowBusy = false;
            ActiveWorkflowLabel = "No command running";
        }
    }

    private void AddWorkflowConsoleLine(string line)
    {
        WorkflowConsoleLines.Insert(0, line);
        while (WorkflowConsoleLines.Count > 80)
        {
            WorkflowConsoleLines.RemoveAt(WorkflowConsoleLines.Count - 1);
        }
    }

    private PointCollection BuildCurve(IEnumerable<double> values)
    {
        var points = new PointCollection();
        var list = values.ToList();
        const double width = 640;
        const double height = 180;
        const double left = 24;
        const double top = 16;
        var stepWidth = width / Math.Max(1, list.Count - 1);

        for (var i = 0; i < list.Count; i++)
        {
            var x = left + (stepWidth * i);
            var y = top + ((1 - list[i]) * height);
            points.Add(new(x, y));
        }

        return points;
    }

    private void AddArchitectureStage(string label, string title, string summary, Brush brush)
    {
        ArchitectureStages.Add(new()
        {
            StepLabel = label,
            Title = title,
            Summary = summary,
            AccentBrush = brush,
        });
    }

    private static bool IsCompletedStage(WorkspaceStageInfo? stage) =>
        stage is not null && (
            stage.Status.Equals("completed", StringComparison.OrdinalIgnoreCase)
            || stage.Status.Equals("completed_with_failures", StringComparison.OrdinalIgnoreCase));

    private static double Clamp(double value) => Math.Max(0.45, Math.Min(0.97, value));

    private static string Lookup(IEnumerable<ChoiceItem> items, string key) =>
        items.FirstOrDefault(item => item.Key == key)?.Label ?? key;

    private void RefreshTrainingExecutionNarrative()
    {
        var runtimeBackends = TrainingStats.FirstOrDefault(card =>
            card.Label.Equals("Runtime backends", StringComparison.OrdinalIgnoreCase));
        var recommendationRuntime = TrainingStats.FirstOrDefault(card =>
            card.Label.Equals("Recommendation runtime", StringComparison.OrdinalIgnoreCase));
        var graphCoverage = TrainingStats.FirstOrDefault(card =>
            card.Label.Equals("Graph coverage", StringComparison.OrdinalIgnoreCase));

        var runtimeText = runtimeBackends is null
            ? "Runtime backend visibility is not available yet."
            : $"Detected backends: {runtimeBackends.Value}.";
        var modeText = SelectedTrainingExecutionModeKey switch
        {
            "prefer_native" => "Requested mode: native-only execution.",
            "safe_baseline" => "Requested mode: safe executable baseline.",
            _ => "Requested mode: automatic runtime-aware execution."
        };
        var executionText = recommendationRuntime is null
            ? "Export a recommendation to determine how the selected model family will execute on this machine."
            : $"Execution mode: {recommendationRuntime.Value}. {recommendationRuntime.Caption}";
        var graphText = graphCoverage is null
            ? string.Empty
            : $" Current graph coverage signal: {graphCoverage.Value}.";

        TrainingExecutionNarrative = $"{runtimeText} {modeText} {executionText}{graphText}".Trim();
    }

    private IReadOnlyList<string> ResolveGraphExportFormats()
    {
        return SelectedGraphExportKey switch
        {
            "interop" => new[] { "pyg", "dgl", "networkx" },
            "audit" => new[] { "networkx" },
            _ => new[] { "pyg", "networkx" },
        };
    }
}
