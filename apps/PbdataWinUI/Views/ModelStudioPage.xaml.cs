namespace PbdataWinUI.Views;

public sealed partial class ModelStudioPage : Page
{
    public ModelStudioPage()
    {
        InitializeComponent();
    }

    public DemoHubViewModel ViewModel => App.Demo;
}
