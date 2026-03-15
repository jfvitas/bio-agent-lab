namespace PbdataWinUI.Views;

public sealed partial class OutputsPage : Page
{
    public OutputsPage()
    {
        InitializeComponent();
    }

    public DemoHubViewModel ViewModel => App.Demo;
}
