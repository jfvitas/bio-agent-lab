using Microsoft.UI.Composition.SystemBackdrops;
using Microsoft.UI.Xaml.Media.Animation;
using Microsoft.UI.Xaml.Navigation;
using PbdataWinUI.ViewModels;

namespace PbdataWinUI;

public partial class App : Application
{
    private Window? _window;

    public App()
    {
        InitializeComponent();
    }

    public static DemoHubViewModel Demo { get; } = new();

    protected override void OnLaunched(LaunchActivatedEventArgs e)
    {
        _window ??= new Window();
        _window.Title = "pbdata demo - Protein Binding Data Platform";

        if (_window.Content is not Frame rootFrame)
        {
            rootFrame = new Frame();
            rootFrame.NavigationFailed += OnNavigationFailed;
            _window.Content = rootFrame;
        }

        if (_window.SystemBackdrop is null)
        {
            _window.SystemBackdrop = new MicaBackdrop();
        }

        _ = rootFrame.Navigate(typeof(MainPage), e.Arguments, new DrillInNavigationTransitionInfo());
        _window.Activate();
    }

    private static void OnNavigationFailed(object sender, NavigationFailedEventArgs e)
    {
        throw new InvalidOperationException("Failed to load page " + e.SourcePageType.FullName);
    }
}
