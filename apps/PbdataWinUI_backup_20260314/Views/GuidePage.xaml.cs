namespace PbdataWinUI.Views;

public sealed partial class GuidePage : Page
{
    public GuidePage()
    {
        InitializeComponent();
    }

    public DemoHubViewModel ViewModel => App.Demo;

    private void OpenRecommendedPage_Click(object sender, RoutedEventArgs e)
    {
        MainPage.CurrentShell?.NavigateToPage(ViewModel.CurrentRecommendedPageKey);
    }
}
