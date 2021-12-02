package cs3743;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.*;

////////////////////////////
// Adminer login
// Your login: ydh648
//Your pw: vXPNlWCLqs16uipFSnR0

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
            //crete select statement
            preparedStatement = connect.prepareStatement
                    ("select c.* from Customer c");
            //preparedStatement.setString(1, "20181Sp");
            resultSet = preparedStatement.executeQuery();
            printCustomers("Beginning Customers",resultSet);
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
        System.out.printf("   %-8s%-20s       %-5s %-11s %-2s\n"
                , "CustNr", "Name", "State", "Birth Dt", "Gender");
        // your code
        while (resultSet.next()) {
            // It is possible to get the columns via name
            // also possible to get the columns via the column number
            // which starts at 1

            //String timeStr;  // can be null

            int custNr = resultSet.getInt("custNr");
            String name = resultSet.getString("name");
            if (name == null)
                name = "---";
            String baseLoc = resultSet.getString("baseLoc");
            if (baseLoc == null)
                baseLoc = "---";
            String birthDt = resultSet.getString("birthDt");          // can be null
            if (birthDt == null) {
                birthDt = "---";
            }
            String gender = resultSet.getString("gender");      // can be null
            if (gender == null)
                gender = "---";
            System.out.printf("   %-8s%-20s       %-5s %-11s %-2s\n"
                    , custNr
                    , name
                    , baseLoc
                    , birthDt
                    , gender);
        }

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
