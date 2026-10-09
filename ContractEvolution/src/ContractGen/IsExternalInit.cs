// netstandard2.0 не знает про init-свойства, которые нужны рекордам
namespace System.Runtime.CompilerServices
{
    internal static class IsExternalInit { }
}
