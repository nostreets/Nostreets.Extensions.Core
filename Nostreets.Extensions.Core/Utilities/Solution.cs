using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Nostreets.Extensions.Utilities
{
    public class Solution
    {

        static Solution()
        {
            s_SolutionParser = Type.GetType("Microsoft.Build.Construction.SolutionParser, Microsoft.Build, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a", false, false);

            if (s_SolutionParser != null)
            {
                s_SolutionParser_solutionReader = s_SolutionParser.GetProperty("SolutionReader", BindingFlags.NonPublic | BindingFlags.Instance);
                s_SolutionParser_projects = s_SolutionParser.GetProperty("Projects", BindingFlags.NonPublic | BindingFlags.Instance);
                s_SolutionParser_parseSolution = s_SolutionParser.GetMethod("ParseSolution", BindingFlags.NonPublic | BindingFlags.Instance);
            }
        }

        public Solution(string solutionFileName)
        {
            if (s_SolutionParser == null)
                throw new InvalidOperationException("Can not find type 'Microsoft.Build.Construction.SolutionParser' are you missing a assembly reference to 'Microsoft.Build.dll'?");


            var solutionParser = s_SolutionParser.GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic).First().Invoke(null);


            using (var streamReader = new StreamReader(solutionFileName))
            {
                s_SolutionParser_solutionReader.SetValue(solutionParser, streamReader, null);
                s_SolutionParser_parseSolution.Invoke(solutionParser, null);
            }

            var projects = new List<Project>();
            var array = (Array)s_SolutionParser_projects.GetValue(solutionParser, null);

            for (int i = 0; i < array.Length; i++)
                projects.Add(new Project(array.GetValue(i)));

            Projects = projects;
        }

        static readonly Type s_SolutionParser;
        static readonly PropertyInfo s_SolutionParser_solutionReader;
        static readonly MethodInfo s_SolutionParser_parseSolution;
        static readonly PropertyInfo s_SolutionParser_projects;

        public List<Project> Projects { get; private set; }


        
    }

    [DebuggerDisplay("{ProjectName}, {RelativePath}, {ProjectGuid}")]
    public class Project
    {
        static Project()
        {
            _projectInSolution = Type.GetType("Microsoft.Build.Construction.ProjectInSolution, Microsoft.Build, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a", false, false);
            if (_projectInSolution != null)
            {
                _projectName = _projectInSolution.GetProperty("ProjectName", BindingFlags.NonPublic | BindingFlags.Instance);
                _projectRelativePath = _projectInSolution.GetProperty("RelativePath", BindingFlags.NonPublic | BindingFlags.Instance);
                _projectGuid = _projectInSolution.GetProperty("ProjectGuid", BindingFlags.NonPublic | BindingFlags.Instance);
            }
        }

        public Project(object solutionProject)
        {
            ProjectName = _projectName.GetValue(solutionProject, null) as string;
            RelativePath = _projectRelativePath.GetValue(solutionProject, null) as string;
            ProjectGuid = _projectGuid.GetValue(solutionProject, null) as string;
        }

        static readonly Type _projectInSolution;
        static readonly PropertyInfo _projectName;
        static readonly PropertyInfo _projectRelativePath;
        static readonly PropertyInfo _projectGuid;

        public string ProjectName { get; private set; }
        public string RelativePath { get; private set; }
        public string ProjectGuid { get; private set; }

    }
}
