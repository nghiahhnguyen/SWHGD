#include <iostream>
#include <chrono>
#include <sstream>
#include <string>
#include <iomanip>

using namespace std;

// ------------------------------------------------
// ------------------------------------------------
long int getSecondsSince1970Until( string dateAndHour ) {

  tm tm = {};
  stringstream ss( dateAndHour );
  ss >> get_time(&tm, "%Y-%m-%d  %H:%M:%S%z");

  chrono::system_clock::time_point tp = chrono::system_clock::from_time_t(mktime(&tm));


  return
    chrono::duration_cast<chrono::seconds>(
                                           tp.time_since_epoch()).count();

} // ()
// ------------------------------------------------
// ------------------------------------------------
long int getMinutesSince1970() {
  chrono::system_clock::time_point now = chrono::system_clock::now();

  return
    chrono::duration_cast<chrono::minutes>( now.time_since_epoch() ).count();
} // ()

// ------------------------------------------------
// ------------------------------------------------
long int getMinutesSince( string dateAndHour ) {

  tm tm = {};
  stringstream ss( dateAndHour );
  ss >> get_time(&tm, "%Y-%m-%d  %H:%M:%S%z");

  chrono::system_clock::time_point then =
    chrono::system_clock::from_time_t(mktime(&tm));

  chrono::system_clock::time_point now = chrono::system_clock::now();

  return
    chrono::duration_cast<chrono::minutes>(
                                           now.time_since_epoch()-
                                           then.time_since_epoch()
                                           ).count();
} // ()


// ------------------------------------------------
// ------------------------------------------------
int main () {

  long int min = getMinutesSince1970Until( "2018-01-12 00:56:38+07" );

  cout << min << endl;


  long int min0 = getMinutesSince1970Until( "2018-01-12 00:56:38+07" );
  long int min1 = getMinutesSince1970Until( "2018-01-12 00:53:05+07" );

  if ( (min1 - min0) != 4 ) {
    cout << " something is wrong " << endl;
  } else {
    cout << " it appears to work !" << endl;
  }

  min0 = getMinutesSince( "1970-01-01 01:00:00" );
  min1 = getMinutesSince1970( );

  if ( (min1 - min0) != 0 ) {
    cout << " something is wrong " << endl;
  } else {
    cout << " it appears to work !" << endl;
  }

}