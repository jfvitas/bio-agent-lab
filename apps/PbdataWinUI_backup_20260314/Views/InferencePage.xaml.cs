namespace PbdataWinUI.Views;

public sealed partial class InferencePage : Page
{
    public InferencePage()
    {
        InitializeComponent();
    }

    public DemoHubViewModel ViewModel => App.Demo;
}
