namespace PbdataWinUI.Views;

public sealed partial class DatasetPage : Page
{
    public DatasetPage()
    {
        InitializeComponent();
    }

    public DemoHubViewModel ViewModel => App.Demo;
}
