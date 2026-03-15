using System.Collections.Generic;
using System.Linq;

namespace PbdataWinUI.Views;

public sealed partial class MainPage : Page
{
    private readonly Dictionary<string, Type> _pages = new()
    {
        ["Guide"] = typeof(GuidePage),
        ["Dataset"] = typeof(DatasetPage),
        ["Model"] = typeof(ModelStudioPage),
        ["Inference"] = typeof(InferencePage),
        ["Outputs"] = typeof(OutputsPage),
    };

    public MainPage()
    {
        InitializeComponent();
        Loaded += OnLoaded;
    }

    public DemoHubViewModel ViewModel => App.Demo;

    private void OnLoaded(object sender, RoutedEventArgs e)
    {
        NavigateTo("Guide");
    }

    private void OnSelectionChanged(NavigationView sender, NavigationViewSelectionChangedEventArgs args)
    {
        var item = args.SelectedItemContainer as NavigationViewItem;
        if (item?.Tag is string key)
        {
            NavigateTo(key);
        }
    }

    private void OnOpenRecommendedSection(object sender, RoutedEventArgs e)
    {
        NavigateTo(ViewModel.CurrentRecommendedPageKey);
    }

    private void OnExecuteRecommendedStep(object sender, RoutedEventArgs e)
    {
        NavigateTo(ViewModel.CurrentRecommendedPageKey);
        ViewModel.ExecuteCurrentStepActionCommand.Execute(null);
    }

    private void NavigateTo(string key)
    {
        if (!_pages.TryGetValue(key, out var pageType))
        {
            return;
        }

        if (ContentFrame.CurrentSourcePageType != pageType)
        {
            ContentFrame.Navigate(pageType);
        }

        var navItem = ShellNav.MenuItems
            .OfType<NavigationViewItem>()
            .FirstOrDefault(item => Equals(item.Tag, key));

        if (navItem is not null && !ReferenceEquals(ShellNav.SelectedItem, navItem))
        {
            ShellNav.SelectedItem = navItem;
        }
    }
}
