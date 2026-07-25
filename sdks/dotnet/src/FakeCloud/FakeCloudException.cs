namespace FakeCloud;

/// <summary>
/// Thrown when a fakecloud introspection endpoint returns a non-2xx response,
/// or when the request fails at the network layer (status -1).
/// </summary>
public class FakeCloudException : Exception
{
    public int Status { get; }
    public string Body { get; }

    public FakeCloudException(int status, string body)
        : base($"fakecloud API error ({status}): {body}")
    {
        Status = status;
        Body = body;
    }
}
