using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using HtmlAgilityPack;
using ScrapySharp.Network;
using ScrapySharp.Extensions;
using Rye.Data;

namespace Rye.Exchange
{
    
    public class WebProvider
    {

        private ScrapingBrowser _Browser;
        private WebPage _Page;

        public WebProvider()
        {
            this._Browser = new ScrapingBrowser();
            this._Page = null;
        }

        public bool PageIsNull
        {
            get { return this._Page == null; }
        }

        public void NavigateURL(string Url)
        {
            this._Page = this._Browser.NavigateToPage(new Uri(Url));
        }

        public string Extract(string Path, int Index)
        {

            HtmlNode[] nodes = this._Page.Html.CssSelect(Path).ToArray();

            if (Index < nodes.Length)
            {
                return nodes[Index].InnerHtml;
            }
            return null;

        }

        public void Sink(string Path)
        {

            this._Page.SaveSnapshot(Path);

        }

        // Static methods //
        public static void HTTP_Request_Get(string URL, string Path)
        {

            try
            {

                // Open a writer
                using (Stream writter = File.Create(Path))
                {

                    // Create the web request
                    System.Net.WebRequest req = System.Net.HttpWebRequest.Create(URL);

                    // Dump to the output writer
                    using (Stream reader = req.GetResponse().GetResponseStream())
                    {
                        reader.CopyTo(writter);
                    }


                }

            }
            catch
            {

            }



        }

        public static void HTTP_Request_Post(string URL, string Path, string PostString)
        {


            try
            {

                // Open a writer
                using (Stream writter = File.Create(Path))
                {

                    // Create the web request
                    System.Net.WebRequest req = System.Net.HttpWebRequest.Create(URL);

                    // Set the posting attributes //
                    req.Method = "POST";
                    req.ContentType = "application/x-www-form-urlencoded";

                    // Set the posting variables //
                    byte[] hash = System.Text.Encoding.ASCII.GetBytes(PostString);
                    req.ContentLength = hash.Length;

                    // Write the post data to a stream //
                    using (Stream post = req.GetRequestStream())
                    {
                        post.Write(hash, 0, hash.Length);
                    }

                    // Dump to the output writer
                    using (Stream reader = req.GetResponse().GetResponseStream())
                    {
                        reader.CopyTo(writter);
                    }

                }

            }
            catch
            {

            }



        }


    }

}
