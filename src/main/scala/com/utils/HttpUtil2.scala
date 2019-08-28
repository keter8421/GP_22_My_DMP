package com.utils

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

object HttpUtil2 {
  def get(url:String): String ={
//    val client: CloseableHttpClient = HttpClients.createDefault()
    val client = new DefaultHttpClient()
    val get = new HttpGet(url)

    val response = client.execute(get)
    val res: String = EntityUtils.toString(response.getEntity,"UTF-8")

    res
  }
}

