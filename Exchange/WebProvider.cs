using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;

namespace Rye.Exchange
{
    
    public class WebProvider
    {

        private CookieContainer _Cookies;
        private HttpWebRequest _Request;
        private HttpWebResponse _Response;

        private byte[] _Cache;

        public WebProvider()
        {
            this._Cookies = new CookieContainer();
            this._Request = null;
            this._Response = null;
        }

        public void Cache(string URL)
        {

            // Open a writer
            using (MemoryStream writter = new MemoryStream())
            {

                // Create the web request
                this._Request = (HttpWebRequest)HttpWebRequest.Create(URL);
                this._Request.CookieContainer = this._Cookies;

                // Dump to the output writer
                this._Response = (HttpWebResponse)this._Request.GetResponse();
                using (Stream reader = this._Response.GetResponseStream())
                {
                    reader.CopyTo(writter);
                }

                this._Cache = writter.ToArray();
            
            }


        }

        public void Post(string URL, string Post)
        {

            // Open a writer
            using (MemoryStream writter = new MemoryStream())
            {

                // Create the web request
                this._Request = (HttpWebRequest)HttpWebRequest.Create(URL);
                this._Request.CookieContainer = this._Cookies;

                // Set the posting attributes //
                this._Request.Method = "POST";
                this._Request.ContentType = "application/x-www-form-urlencoded";

                // Set the posting variables //
                byte[] hash = System.Text.Encoding.ASCII.GetBytes(Post);
                this._Request.ContentLength = hash.Length;

                // Write the post data to a stream //
                using (Stream post = this._Request.GetRequestStream())
                {
                    post.Write(hash, 0, hash.Length);
                }

                // Dump to the output writer
                this._Response = (HttpWebResponse)this._Request.GetResponse();
                using (Stream reader = this._Response.GetResponseStream())
                {
                    reader.CopyTo(writter);
                }

                this._Cache = writter.ToArray();
            
            }

        }

        public void Dump(string Path)
        {

            using (Stream s = File.Create(Path))
            {

                if (this._Cache != null)
                {
                    s.Write(this._Cache, 0, this._Cache.Length);
                }

            }

        }

    }

}
