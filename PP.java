package lbj4;

import java.sql.*;
import java.io.*;
public class PP {
	
	// Название драйвера
	  String driverName = "com.mysql.jdbc.Driver";
	 // Параметры соединения с базой данных
	  String serverName = "127.0.0.1:50001";
	  String mydatabase = "publisher";
	   String url = "jdbc:mysql://" + serverName + "/"+ mydatabase;
	  String username = "manas";
	   String password = "123";	
	  Connection connection = null ;
	  
	  
public String DbOutput (Statement stmt,ResultSet rs, String itog) throws SQLException		 
{
	 System.out.println("\n(Eskmo deleted)");
	
String dbrecord="";
	
	// Число полей таблицы 
	String qucols = "SHOW COLUMNS FROM publisher";
	int cols = stmt.executeUpdate(qucols);

	// Строка с запросом на выборку
	String query = "Select * FROM publisher";
	// Результат запроса
	 rs = stmt.executeQuery(query);
	 
	// Выборка записей из таблицы
	while (rs.next())
	{
	for(int i = 1; i<=cols; i++)
	dbrecord += rs.getString(i) + " \t ";
	
	itog += dbrecord+"\n";
	
	 System.out.println(dbrecord); // Вывод строки
	 dbrecord = "";
	 } // конец цикла выборки записей
	return itog;
}

////////////////////////////////////////////
public String MoscowSelection (Statement stmt,ResultSet rs, String itog) throws SQLException	
{
	System.out.println("\n(Moscow selected)");
	
	String dbrecord="";
	
	// Число полей таблицы 
	String qucols = "SHOW COLUMNS FROM publisher";
	int cols = stmt.executeUpdate(qucols);

	// Строка с запросом на выборку
	String query = "Select * FROM publisher WHERE City = 'Moscow'";
	// Результат запроса
	 rs = stmt.executeQuery(query);
	 
	// Выборка записей из таблицы
	while (rs.next())
	{
	for(int i = 1; i<=cols; i++)
	dbrecord += rs.getString(i) + " \t ";
	
	itog += dbrecord+"\n";
	
	 System.out.println(dbrecord); // Вывод строки
	 dbrecord = "";
	 } // конец цикла выборки записей
	return itog;
}

////////////////////////////////////////////////

public void DeleteEskmoLine (Statement stmt,ResultSet rs1) throws SQLException
{
	String dbrecord="";
	
	// Число полей таблицы 
	String qucols = "SHOW COLUMNS FROM publisher";
	
	// Строка с запросом на выборку
	PreparedStatement st = connection.prepareStatement( "DELETE FROM publisher WHERE Name = 'Eskmo'");//Select * FROM publisher";
	// Результат запроса
	 st.executeUpdate();

}

////////////////////////////////////////////////

public void WrToFile (String itog)
{
	try(FileWriter writer = new FileWriter("PublisherSQL.txt", true))
    {
        writer.write(itog);
        // запись по символам
        writer.append('\n');
        //writer.append('E');
         
        writer.flush(); 
       
    }
    catch(IOException ex){
         
        System.out.println(ex.getMessage());
    } 
}

////////////////////////////////////////////
public void RelatTable (Statement stmt,ResultSet rs) throws SQLException	
{
System.out.println("*** Unifying 2 tables by Foreign Key (p_id) ***\n");

String dbrecord="";

// Число полей таблицы 
String qucols = "SHOW COLUMNS FROM publisher";
int cols = stmt.executeUpdate(qucols);

// Строка с запросом на выборку
String query = "select wr_id, writers.p_id, Author, publisher, city  from publisher join writers on writers.p_id = publisher.p_id where writers.p_id = 1993";
// Результат запроса
rs = stmt.executeQuery(query);

// Выборка записей из таблицы
while (rs.next())
{
for(int i = 1; i<=cols; i++)
dbrecord += rs.getString(i) + " \t ";

//itog += dbrecord+"\n";

System.out.println(dbrecord); // Вывод строки
dbrecord = "";
} // конец цикла выборки записей
//return itog;
}

////////////////////////////////////////////////
	
////////////////////////////////////////////////////	
public static void main(String args[]) {
 //Connection connection = null;
try {
	PP pp1 = new PP();
	
	Class.forName(pp1.driverName);
	pp1.connection = DriverManager.getConnection(pp1.url, pp1.username, pp1.password); 

	/* Подключение к БД
	 * После успешного подключения создаем объект Statement,
	 *  который будет использоваться для выполнения
	 *   запросов к базе данных
	*/
		 Statement stmt = pp1.connection.createStatement();
 /*Объявляем переменную ResultSet для получения результата
  *  запроса и выполняем запрос: */
		 ResultSet rs = null;
	
	String itog = "";
	
	pp1.RelatTable(stmt, rs);
	
//pp1.WrToFile (pp1.MoscowSelection (stmt,rs, itog));
	
//ResultSet rs1 = null;
//pp1.DeleteEskmoLine(stmt, rs);


//pp1.WrToFile(pp1.DbOutput(stmt, rs, itog));

pp1.connection.close(); // Соединение закрыто
 } // Конец try
// Обработка исключений
catch (ClassNotFoundException e) {
 e.printStackTrace();
 // Не найден драйвер баз данных
 } catch (SQLException e) {
 e.printStackTrace();
 // Нет соединения с базой данных
 }
}
} 