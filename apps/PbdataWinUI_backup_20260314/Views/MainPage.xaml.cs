namespace PbdataWinUI.Views;

public sealed partial class MainPage : Page
{
    public MainPage()
    {
        InitializeComponent();
    }

    public DemoHubViewModel ViewModel => App.Demo;

    private void OnSelectionChanged(NavigationView sender, NavigationViewSelectionChangedEventArgs args)
    {
        var item = args.SelectedItemContainer as NavigationViewItem ?? args.SelectedItem as NavigationViewItem;
        if (item?.Content is string label)
        {
            SectionSummaryText.Text = $"{label} section selected.";
        }
    }
}
