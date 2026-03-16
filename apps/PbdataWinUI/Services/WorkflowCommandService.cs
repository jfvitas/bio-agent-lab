using System.Diagnostics;

namespace PbdataWinUI.Services;

public sealed class WorkflowCommandService
{
    public async Task<WorkflowCommandResult> RunAsync(
        string workspaceRoot,
        IReadOnlyList<string> commandArguments,
        Action<string>? onOutput = null,
        CancellationToken cancellationToken = default)
    {
        var root = Path.GetFullPath(workspaceRoot);
        var launcher = ResolvePythonLauncher(root);
        var output = new List<string>();

        var startInfo = new ProcessStartInfo
        {
            FileName = launcher.FileName,
            WorkingDirectory = root,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        foreach (var argument in launcher.PrefixArguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        startInfo.ArgumentList.Add("-m");
        startInfo.ArgumentList.Add("pbdata");
        startInfo.ArgumentList.Add("--storage-root");
        startInfo.ArgumentList.Add(root);

        foreach (var argument in commandArguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        var pythonPath = Path.Combine(root, "src");
        startInfo.Environment["PYTHONPATH"] = string.IsNullOrWhiteSpace(
            startInfo.Environment.TryGetValue("PYTHONPATH", out var existingPath) ? existingPath : null)
            ? pythonPath
            : $"{pythonPath};{existingPath}";

        using var process = new Process { StartInfo = startInfo };
        try
        {
            process.Start();
        }
        catch (Exception ex)
        {
            return new WorkflowCommandResult
            {
                ExitCode = -1,
                Succeeded = false,
                OutputLines = new[] { $"Failed to start command runner: {ex.Message}" },
                Executable = startInfo.FileName,
                Arguments = startInfo.ArgumentList.ToArray(),
            };
        }

        var stdoutTask = ConsumeReaderAsync(process.StandardOutput, output, onOutput, cancellationToken);
        var stderrTask = ConsumeReaderAsync(process.StandardError, output, onOutput, cancellationToken);

        await Task.WhenAll(stdoutTask, stderrTask, process.WaitForExitAsync(cancellationToken));

        return new WorkflowCommandResult
        {
            ExitCode = process.ExitCode,
            Succeeded = process.ExitCode == 0,
            OutputLines = output,
            Executable = startInfo.FileName,
            Arguments = startInfo.ArgumentList.ToArray(),
        };
    }

    public async Task<WorkflowCommandResult> RunScriptAsync(
        string workspaceRoot,
        string scriptRelativePath,
        IReadOnlyList<string> scriptArguments,
        Action<string>? onOutput = null,
        CancellationToken cancellationToken = default)
    {
        var root = Path.GetFullPath(workspaceRoot);
        var launcher = ResolvePythonLauncher(root);
        var output = new List<string>();
        var startInfo = new ProcessStartInfo
        {
            FileName = launcher.FileName,
            WorkingDirectory = root,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        foreach (var argument in launcher.PrefixArguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        startInfo.ArgumentList.Add(Path.Combine(root, scriptRelativePath));
        foreach (var argument in scriptArguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        var pythonPath = Path.Combine(root, "src");
        startInfo.Environment["PYTHONPATH"] = string.IsNullOrWhiteSpace(
            startInfo.Environment.TryGetValue("PYTHONPATH", out var existingPath) ? existingPath : null)
            ? pythonPath
            : $"{pythonPath};{existingPath}";

        using var process = new Process { StartInfo = startInfo };
        try
        {
            process.Start();
        }
        catch (Exception ex)
        {
            return new WorkflowCommandResult
            {
                ExitCode = -1,
                Succeeded = false,
                OutputLines = new[] { $"Failed to start script runner: {ex.Message}" },
                Executable = startInfo.FileName,
                Arguments = startInfo.ArgumentList.ToArray(),
            };
        }

        var stdoutTask = ConsumeReaderAsync(process.StandardOutput, output, onOutput, cancellationToken);
        var stderrTask = ConsumeReaderAsync(process.StandardError, output, onOutput, cancellationToken);
        await Task.WhenAll(stdoutTask, stderrTask, process.WaitForExitAsync(cancellationToken));

        return new WorkflowCommandResult
        {
            ExitCode = process.ExitCode,
            Succeeded = process.ExitCode == 0,
            OutputLines = output,
            Executable = startInfo.FileName,
            Arguments = startInfo.ArgumentList.ToArray(),
        };
    }

    private static async Task ConsumeReaderAsync(
        StreamReader reader,
        ICollection<string> output,
        Action<string>? onOutput,
        CancellationToken cancellationToken)
    {
        while (!reader.EndOfStream)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var line = await reader.ReadLineAsync(cancellationToken) ?? string.Empty;
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            lock (output)
            {
                output.Add(line);
            }

            onOutput?.Invoke(line);
        }
    }

    private static PythonLauncher ResolvePythonLauncher(string workspaceRoot)
    {
        var venvPython = Path.Combine(workspaceRoot, ".venv", "Scripts", "python.exe");
        if (File.Exists(venvPython))
        {
            return new PythonLauncher(venvPython, Array.Empty<string>());
        }

        if (CommandExists("py"))
        {
            return new PythonLauncher("py", Array.Empty<string>());
        }

        return new PythonLauncher("python", Array.Empty<string>());
    }

    private static bool CommandExists(string command)
    {
        var pathValue = Environment.GetEnvironmentVariable("PATH") ?? string.Empty;
        foreach (var directory in pathValue.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            try
            {
                var candidate = Path.Combine(directory.Trim(), $"{command}.exe");
                if (File.Exists(candidate))
                {
                    return true;
                }
            }
            catch
            {
                // Ignore malformed PATH segments and continue scanning.
            }
        }

        return false;
    }

    private sealed record PythonLauncher(string FileName, IReadOnlyList<string> PrefixArguments);
}

public sealed class WorkflowCommandResult
{
    public required int ExitCode { get; init; }
    public required bool Succeeded { get; init; }
    public required IReadOnlyList<string> OutputLines { get; init; }
    public required string Executable { get; init; }
    public required IReadOnlyList<string> Arguments { get; init; }
}
