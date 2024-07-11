package spark.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation


class KerberosUtil {
//  private val keytab = getClass.getClassLoader.getResource("260371.keytab").getPath
//  private val krb5 = getClass.getClassLoader.getResource("krb5.conf").getPath
  def krbLoginedUGI(krbUser: String, keytabUrl: String, krb5Url: String): Unit = {

    println(keytabUrl)

    System.setProperty("java.security.krb5.conf", krb5Url)
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
    val configuration = new Configuration
    configuration.set("hadoop.security.authentication", "kerberos")
    UserGroupInformation.setConfiguration(configuration)
    UserGroupInformation.loginUserFromKeytab(krbUser, keytabUrl)
    println(UserGroupInformation.getCurrentUser.getShortUserName)

  }
}
