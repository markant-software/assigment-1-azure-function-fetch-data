using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Assigment1FetchDataFromAPI.Models
{
    public class FetchedData
    {
        public string rowKey { get; set; }
        public bool success { get; set; }
        public DateTime requestTime { get; set; }
        public string content { get; set; } = null;
    }
}
