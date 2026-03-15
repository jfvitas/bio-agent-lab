namespace PbdataWinUI.Views;

public sealed partial class GuidePage : Page
{
    public GuidePage()
    {
        InitializeComponent();
    }

    public DemoHubViewModel ViewModel => App.Demo;
}
