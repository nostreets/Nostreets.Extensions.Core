using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.AspNetCore.Components.RenderTree;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.RenderTree;
using Microsoft.AspNetCore.Components.Rendering;
using System.Text;
using System.Diagnostics.CodeAnalysis;

namespace Nostreets.Extensions.Core.Extend
{
    [SuppressMessage("Usage", "BL0006:Do not use RenderTree types", Justification = "<Pending>")]
    public static class BlazorExtensions
    {
        public static bool TryGetQueryString<T>(this NavigationManager navManager, string key, out T value)
        {
            var uri = navManager.ToAbsoluteUri(navManager.Uri);

            if (QueryHelpers.ParseQuery(uri.Query).TryGetValue(key, out var valueFromQueryString))
            {
                if (typeof(T) == typeof(int) && int.TryParse(valueFromQueryString, out var valueAsInt))
                {
                    value = (T)(object)valueAsInt;
                    return true;
                }

                if (typeof(T) == typeof(string))
                {
                    value = (T)(object)valueFromQueryString.ToString();
                    return true;
                }

                if (typeof(T) == typeof(decimal) && decimal.TryParse(valueFromQueryString, out var valueAsDecimal))
                {
                    value = (T)(object)valueAsDecimal;
                    return true;
                }
            }

            value = default;
            return false;
        }

        public static string ToStringValue(this RenderFragment renderFragment)
        {
            var stringBuilder = new StringBuilder();
            var renderTreeBuilder = new RenderTreeBuilder();

            renderTreeBuilder.Clear();
            renderTreeBuilder.AddContent(0, renderFragment);

            var frames = renderTreeBuilder.GetFrames().Array;
            foreach (var frame in frames)
            {
                frame.ToStringValue(stringBuilder);
            }

            return stringBuilder.ToString();
        }

        public static void ToStringValue(this RenderTreeFrame frame, StringBuilder stringBuilder)
        {
            switch (frame.FrameType)
            {
                case RenderTreeFrameType.Text:
                    stringBuilder.Append(frame.TextContent);
                    break;

                case RenderTreeFrameType.Element:
                    stringBuilder.Append(frame.ElementName);
                    break;

                case RenderTreeFrameType.Component:
                    var componentState = frame.Component;
                    if (componentState is IComponent component)
                    {
                        stringBuilder.Append(component.GetType().Name);
                    }
                    break;
            }
        }

    }
}
