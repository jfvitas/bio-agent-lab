using System.Collections.ObjectModel;
using System.Linq;

namespace PbdataWinUI.ViewModels;

public sealed partial class DemoHubViewModel : BaseViewModel
{
    private readonly List<DemoStep> _steps;

    private string _selectedDatasetProfileKey = "broad";
    private string _selectedBalanceKey = "family";
    private string _selectedEmbeddingKey = "esm2";
    private string _selectedObjectiveKey = "hybrid";
    private string _selectedModelFamilyKey = "hybrid";
    private string _selectedModalityKey = "tri_modal";
    private string _selectedSpeedKey = "balanced";
    private string _selectedInferenceScenarioKey = "novel_pocket";
    private int _currentStepIndex;
    private int _completedStepCount;

    private string _selectedModelHeadline = string.Empty;
    private string _selectedModelPitch = string.Empty;
    private string _datasetNarrative = string.Empty;
    private string _splitNarrative = string.Empty;
    private string _inferenceNarrative = string.Empty;
    private string _runSummary = string.Empty;
    private string _selectedRunName = "Hybrid Fusion - novelty holdout";

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
            new() { Key = "best", Label = "Best accuracy", Caption = "Longer simulated run with stronger cross-family generalization." },
        };

        InferenceScenarios = new ObservableCollection<ChoiceItem>
        {
            new() { Key = "novel_pocket", Label = "Novel pocket screen", Caption = "Demonstrate how the saved model ranks new complexes." },
            new() { Key = "ppi_triage", Label = "PPI triage", Caption = "Rank interface-heavy protein-protein examples." },
            new() { Key = "mutant_scan", Label = "Mutant sensitivity", Caption = "Show how motif and residue changes shift predictions." },
        };

        _steps = new List<DemoStep>
        {
            new() { Number = 1, PageKey = "Guide", Title = "Frame the story", ActionLabel = "Begin workflow", WhyItMatters = "This primes the workspace with a coherent scientific story instead of a pile of controls.", WhatToClick = "Begin workflow", HowToFindIt = "Open Workflow and use the first primary button." },
            new() { Number = 2, PageKey = "Dataset", Title = "Preview representative coverage", ActionLabel = "Preview search", WhyItMatters = "The platform shows broad structural coverage rather than returning a narrow cluster of near-duplicates.", WhatToClick = "Preview Search", HowToFindIt = "Open Dataset and use the first action button." },
            new() { Number = 3, PageKey = "Dataset", Title = "Assemble the graph-ready set", ActionLabel = "Assemble set", WhyItMatters = "This reconciles structure, assay, sequence, and motif context into a balanced product-ready dataset.", WhatToClick = "Assemble Set", HowToFindIt = "Stay in Dataset and use the middle action button." },
            new() { Number = 4, PageKey = "Dataset", Title = "Design the leakage-resistant split", ActionLabel = "Design split", WhyItMatters = "The split logic emphasizes family holdout, motif grouping, and source-aware evaluation.", WhatToClick = "Design Split", HowToFindIt = "Use the third Dataset action button." },
            new() { Number = 5, PageKey = "Model", Title = "Refresh the model path", ActionLabel = "Recommend", WhyItMatters = "Model Studio explains why a given architecture suits the selected modalities and objective.", WhatToClick = "Recommend", HowToFindIt = "Open Models and use the first action button." },
            new() { Number = 6, PageKey = "Model", Title = "Train the candidate", ActionLabel = "Train", WhyItMatters = "The training view shows realistic curves, scores, and architecture tradeoffs.", WhatToClick = "Train", HowToFindIt = "Use the primary training button in Models." },
            new() { Number = 7, PageKey = "Inference", Title = "Run saved-model inference", ActionLabel = "Run inference", WhyItMatters = "This demonstrates how the platform turns a saved model into ranked, explainable predictions.", WhatToClick = "Run Inference", HowToFindIt = "Open Inference Lab and use the primary inference button." },
        };

        TimelineStages = new ObservableCollection<DemoTimelineStage>();
        DatasetStats = new ObservableCollection<StatCard>();
        TrainingStats = new ObservableCollection<StatCard>();
        MetricBars = new ObservableCollection<MetricBar>();
        ArchitectureStages = new ObservableCollection<ArchitectureStage>();
        ComparisonRuns = new ObservableCollection<RunSummary>();
        ArtifactSummaries = new ObservableCollection<ArtifactSummary>();
        Predictions = new ObservableCollection<PredictionSummary>();
        ActivityLog = new ObservableCollection<string>();

        ResetDemo();
    }

    public ObservableCollection<ChoiceItem> DatasetProfiles { get; }
    public ObservableCollection<ChoiceItem> BalanceStrategies { get; }
    public ObservableCollection<ChoiceItem> EmbeddingStrategies { get; }
    public ObservableCollection<ChoiceItem> Objectives { get; }
    public ObservableCollection<ChoiceItem> ModelFamilies { get; }
    public ObservableCollection<ChoiceItem> Modalities { get; }
    public ObservableCollection<ChoiceItem> SpeedProfiles { get; }
    public ObservableCollection<ChoiceItem> InferenceScenarios { get; }

    public ObservableCollection<DemoTimelineStage> TimelineStages { get; }
    public ObservableCollection<StatCard> DatasetStats { get; }
    public ObservableCollection<StatCard> TrainingStats { get; }
    public ObservableCollection<MetricBar> MetricBars { get; }
    public ObservableCollection<ArchitectureStage> ArchitectureStages { get; }
    public ObservableCollection<RunSummary> ComparisonRuns { get; }
    public ObservableCollection<ArtifactSummary> ArtifactSummaries { get; }
    public ObservableCollection<PredictionSummary> Predictions { get; }
    public ObservableCollection<string> ActivityLog { get; }

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

    public string CurrentStepTitle => _steps[_currentStepIndex].Title;
    public string CurrentStepWhy => _steps[_currentStepIndex].WhyItMatters;
    public string CurrentStepClick => _steps[_currentStepIndex].WhatToClick;
    public string CurrentStepFind => _steps[_currentStepIndex].HowToFindIt;
    public string CurrentRecommendedPageKey => _steps[_currentStepIndex].PageKey;
    public string CurrentActionLabel => _steps[_currentStepIndex].ActionLabel;
    public string ProgressLabel => $"{_completedStepCount} of {_steps.Count} guided steps completed";
    public string DemoHeadline => "pbdata turns broad structural evidence into balanced ML-ready datasets, workflow-aware model selection, and explainable prediction outputs.";
    public string DemoDisclaimer => "This preview uses representative generated content to show intended behavior without waiting on long-running ingestion or training jobs.";

    public string DatasetNarrative
    {
        get => _datasetNarrative;
        private set => SetProperty(ref _datasetNarrative, value);
    }

    public string SplitNarrative
    {
        get => _splitNarrative;
        private set => SetProperty(ref _splitNarrative, value);
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
    private void StartGuidedDemo()
    {
        ResetDemo();
        AppendLog("Workflow", "Workflow preview restarted with a broad, representative discovery scenario.");
        AdvanceToStep(1);
    }

    [RelayCommand]
    private void ExecuteCurrentStepAction()
    {
        switch (_currentStepIndex)
        {
            case 0:
                StartGuidedDemo();
                break;
            case 1:
                PreviewSearch();
                break;
            case 2:
                AssembleDataset();
                break;
            case 3:
                DesignSplit();
                break;
            case 4:
                RecommendModel();
                break;
            case 5:
                TrainModel();
                break;
            default:
                RunInference();
                break;
        }
    }

    [RelayCommand]
    private void RunCompleteStory()
    {
        PreviewSearch();
        AssembleDataset();
        DesignSplit();
        RecommendModel();
        TrainModel();
        CompareRuns();
        RunInference();
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
        _selectedInferenceScenarioKey = "novel_pocket";

        OnPropertyChanged(nameof(SelectedDatasetProfileKey));
        OnPropertyChanged(nameof(SelectedBalanceKey));
        OnPropertyChanged(nameof(SelectedEmbeddingKey));
        OnPropertyChanged(nameof(SelectedObjectiveKey));
        OnPropertyChanged(nameof(SelectedModelFamilyKey));
        OnPropertyChanged(nameof(SelectedModalityKey));
        OnPropertyChanged(nameof(SelectedSpeedKey));
        OnPropertyChanged(nameof(SelectedInferenceScenarioKey));

        ActivityLog.Clear();
        AppendLog("Workspace", "Workspace seeded with a representative multi-source protein-binding story.");

        RefreshDatasetProjection();
        RefreshModelProjection();
        RefreshInferenceProjection();
        RebuildTimeline();
        RebuildArtifacts("Ready for preview");
        NotifyGuideState();
    }

    [RelayCommand]
    private void PreviewSearch()
    {
        AppendLog("Search", "Representative search expanded across protein-protein, protein-ligand, and single-protein control strata.");
        AdvanceToStep(2);
    }

    [RelayCommand]
    private void AssembleDataset()
    {
        RefreshDatasetProjection();
        AppendLog("Dataset", $"Built a graph-ready {Lookup(DatasetProfiles, SelectedDatasetProfileKey)} set with {DatasetStats.FirstOrDefault()?.Value ?? "tens of thousands"} paired examples.");
        AdvanceToStep(3);
        RebuildArtifacts("Dataset package prepared");
    }

    [RelayCommand]
    private void DesignSplit()
    {
        SplitNarrative = SelectedBalanceKey switch
        {
            "novelty" => "Split design now emphasizes novel folds, rare motif families, and source-held-out assay contexts so the benchmark reads as a generalization test rather than memorization.",
            "assay" => "Split design now equalizes assay regimes and source provenance to prevent one measurement family from dominating the evaluation story.",
            _ => "Split design now groups by family, motif, and source so near-duplicate proteins and mutation clusters stay on one side of the evaluation boundary.",
        };
        AppendLog("Split", "Leakage-resistant split designed with family, motif, and source-aware grouping.");
        AdvanceToStep(4);
        RebuildArtifacts("Split manifest prepared");
    }

    [RelayCommand]
    private void RecommendModel()
    {
        if (SelectedModelFamilyKey == "auto")
        {
            SelectedModelFamilyKey = SelectedObjectiveKey switch
            {
                "interface" => "gnn",
                "screen" => "xgb",
                _ => "hybrid",
            };
        }

        RefreshModelProjection();
        AppendLog("Model Studio", $"Recommended {Lookup(ModelFamilies, SelectedModelFamilyKey)} for the selected modality blend.");
        AdvanceToStep(5);
    }

    [RelayCommand]
    private void TrainModel()
    {
        RefreshModelProjection();
        SelectedRunName = $"{Lookup(ModelFamilies, SelectedModelFamilyKey)} - {Lookup(SpeedProfiles, SelectedSpeedKey)}";
        RunSummary = SelectedModelFamilyKey switch
        {
            "rf" => "Fast, interpretable baseline with clear feature attributions and a slightly lower ceiling on novel-family generalization.",
            "xgb" => "Boosted tabular learner that improves ranking quality and calibration while keeping the workflow lightweight.",
            "gnn" => "Graph-heavy architecture emphasizing residue neighborhoods and contact topology for interface-rich tasks.",
            _ => "Fusion model that blends structure graphs, embeddings, and assay descriptors to produce the strongest overall platform performance.",
        };

        AppendLog("Training", $"Trained {SelectedRunName} and produced realistic chart outputs, scorecards, and saved artifacts.");
        AdvanceToStep(6);
        RebuildArtifacts("Training report prepared");
        RebuildComparisonRuns();
    }

    [RelayCommand]
    private void CompareRuns()
    {
        RebuildComparisonRuns();
        AppendLog("Comparison", "Compared the current run against alternate model families with plausible tradeoffs in speed, interpretability, and accuracy.");
        AdvanceToStep(6);
        RebuildArtifacts("Comparison report prepared");
    }

    [RelayCommand]
    private void RunInference()
    {
        RefreshInferenceProjection();
        AppendLog("Inference", $"Ran saved-model inference for the {Lookup(InferenceScenarios, SelectedInferenceScenarioKey)} scenario.");
        AdvanceToStep(_steps.Count);
        RebuildArtifacts("Inference packet prepared");
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

    private void RefreshDatasetProjection()
    {
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

        DatasetNarrative = $"{profile.note} {balanceText} Embedding path: {Lookup(EmbeddingStrategies, SelectedEmbeddingKey)}.";
        SplitNarrative = $"Current leakage risk reads as {profile.leakage}. The split logic keeps sequence-near duplicates, fold relatives, and source clusters from leaking across train and evaluation.";
    }

    private void RefreshModelProjection()
    {
        var family = SelectedModelFamilyKey;
        var objective = SelectedObjectiveKey;
        var modality = SelectedModalityKey;
        var speed = SelectedSpeedKey;
        var embedding = SelectedEmbeddingKey;

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

        SelectedModelHeadline = family switch
        {
            "rf" => "Tree ensemble baseline with compact, interpretable feature flows",
            "xgb" => "Boosted tabular learner tuned for ranking-heavy binding screens",
            "gnn" => "Residue-contact graph model emphasizing topology and neighborhood signal",
            "auto" => "Automatically chosen path balancing speed, interpretability, and modality fit",
            _ => "Hybrid fusion model combining graph, descriptor, and embedding branches",
        };

        SelectedModelPitch = $"Objective: {Lookup(Objectives, SelectedObjectiveKey)}. Modality blend: {Lookup(Modalities, SelectedModalityKey)}. Embedding strategy: {Lookup(EmbeddingStrategies, SelectedEmbeddingKey)}.";

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

        ArchitectureStages.Clear();
        AddArchitectureStage("1", family switch { "gnn" => "Residue/contact graph", "rf" or "xgb" => "Engineered descriptor table", _ => "Graph + descriptor intake" }, "The app reconciles structure, motif, assay, and embedding context into one coherent input layer.", DemoPalette.Aqua);
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

    private void RefreshInferenceProjection()
    {
        Predictions.Clear();

        if (SelectedInferenceScenarioKey == "ppi_triage")
        {
            Predictions.Add(new() { PairLabel = "HSP90 / CDC37 interface", Score = "0.912", Confidence = "High", Rationale = "Strong contact density, compatible motif neighborhoods, and family-consistent interface geometry.", RiskNote = "Watch for scaffold over-representation in chaperone-rich folds." });
            Predictions.Add(new() { PairLabel = "PD-1 / PD-L1 variant", Score = "0.873", Confidence = "High", Rationale = "Interface residues align well with learned hotspot neighborhoods and calibration remains stable.", RiskNote = "Mutation cluster is close to seen immune checkpoint families." });
            Predictions.Add(new() { PairLabel = "RAS / RAF pocketed assembly", Score = "0.801", Confidence = "Medium", Rationale = "Good graph topology match, but assay context is thinner than the top-ranked examples.", RiskNote = "Sparser assay support than the top two predictions." });
            InferenceNarrative = "Inference Lab is showing an interface-heavy triage view, where the saved model ranks protein-protein complexes and surfaces likely reasons for strong interaction confidence.";
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
            InferenceNarrative = "Inference Lab is showing a novel pocket screening story, where the saved model surfaces ranked hits plus the motifs and structural reasons behind each score.";
        }
    }

    private void RebuildComparisonRuns()
    {
        ComparisonRuns.Clear();

        ComparisonRuns.Add(new() { Name = "Hybrid Fusion - novelty holdout", Family = "Hybrid Fusion", HeadlineMetric = "AUPRC 0.846", SupportMetric = "AUROC 0.921", Note = "Best overall transfer in the current preview run set.", AccentBrush = DemoPalette.Aqua });
        ComparisonRuns.Add(new() { Name = "Graph Neural Net - interface heavy", Family = "Graph Neural Net", HeadlineMetric = "AUPRC 0.811", SupportMetric = "AUROC 0.901", Note = "Excellent interface reasoning, slightly weaker calibration than fusion.", AccentBrush = DemoPalette.Blue });
        ComparisonRuns.Add(new() { Name = "XGBoost - descriptor baseline", Family = "XGBoost", HeadlineMetric = "AUPRC 0.762", SupportMetric = "AUROC 0.871", Note = "Useful fast baseline with strong interpretability and lower overhead.", AccentBrush = DemoPalette.Gold });
    }

    private void RebuildArtifacts(string status)
    {
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
            var statusLabel = i < _completedStepCount ? "Completed" : i == _currentStepIndex ? "Recommended now" : "Queued";
            var brush = i < _completedStepCount ? DemoPalette.Aqua : i == _currentStepIndex ? DemoPalette.Gold : DemoPalette.Slate;

            TimelineStages.Add(new()
            {
                Title = step.Title,
                Summary = step.WhyItMatters,
                StatusLabel = statusLabel,
                PageKey = step.PageKey,
                ActionLabel = step.ActionLabel,
                AccentBrush = brush,
            });
        }
    }

    private void AppendLog(string source, string message)
    {
        ActivityLog.Insert(0, $"[{DateTime.Now:HH:mm}] {source}: {message}");
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

    private static double Clamp(double value) => Math.Max(0.45, Math.Min(0.97, value));

    private static string Lookup(IEnumerable<ChoiceItem> items, string key) =>
        items.FirstOrDefault(item => item.Key == key)?.Label ?? key;
}
