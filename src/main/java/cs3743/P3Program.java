package cs3743;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.*;

public class P3Program 
{
    private Connection connect = null;
    
    private Statement statement = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;
    public static final int ER_DUP_ENTRY = 1062;
    public static final int ER_DUP_ENTRY_WITH_KEY_NAME = 1586;
    public static final String[] strPropertyIdM =
    {   "MTNDDD"
       ,"NYCCC"
       ,"HOMEJJJ"
       ,"END"
    };
    
    public P3Program (String user, String password) throws Exception {
        try {
            // This will load the MySQL driver, each DBMS has its own driver
            MysqlDataSource ds = new MysqlDataSource();
            ds.setURL("jdbc:mysql://cs3743.fulgentcorp.com:3306/cs3743_" + user);
            ds.setUser(user);
            ds.setPassword(password);
            this.connect = ds.getConnection();
        } catch (Exception e) {
            throw e;
        } 
        
    }
       
    public void runProgram() throws Exception 
    {
        try 
        {
            // your code
            

        } 
        catch (Exception e) 
        {
            throw e;
        } 
        finally 
        {
            close();
        }

    }                                                                                                                        
    
    private void printCustomers(String title, ResultSet resultSet) throws SQLException 
    {
       // Your output for this must match the format of my sample output exactly. 
       // custNr, name, baseLoc, birthDt, gender
        System.out.printf("%s\n", title);
        // your code
        
    }
    

    // Close the resultSet, statement, preparedStatement, and connect
    private void close() 
    {
        try 
        {
            if (resultSet != null) 
                resultSet.close();

            if (statement != null) 
                statement.close();
            
            if (preparedStatement != null) 
                preparedStatement.close();

            if (connect != null) 
                connect.close();
        } 
        catch (Exception e) 
        {

        }
    }

}
