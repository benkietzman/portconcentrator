// vim600: fdm=marker
/* -*- c++ -*- */
///////////////////////////////////////////
// Port Concentrator
// -------------------------------------
// file       : concentrator.cpp
// author     : Ben Kietzman
// begin      : 2014-02-26
// copyright  : kietzman.org
// email      : ben@kietzman.org
///////////////////////////////////////////

/*! \file concentrator.cpp
* \brief Port Concentrator Daemon
*
* Concentrates incoming parallel socket connections down to one at a time outgoing socket connections.
*/
// {{{ includes
#include <arpa/inet.h>
#include <cerrno>
#include <ctime>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <string>
#include <thread>
#include <vector>
using namespace std;
#include <Central>
#include <Json>
using namespace common;
// }}}
// {{{ defines
#ifdef VERSION
#undef VERSION
#endif
/*! \def VERSION
* \brief Contains the application version number.
*/
#define VERSION "0.1"
/*! \def mUSAGE(A)
* \brief Prints the usage statement.
*/
#define mUSAGE(A) cout << endl << "Usage:  "<< A << " [options]"  << endl << endl << " -c, --conf" << endl << "     Sets the configuration directory." << endl << endl << " -d, --daemon" << endl << "     Turns the process into a daemon." << endl << endl << "     --data" << endl << "     Sets the data directory." << endl << endl << " -e EMAIL, --email=EMAIL" << endl << "     Provides the email address for default notifications." << endl << endl << " -h, --help" << endl << "     Displays this usage screen." << endl << endl << " -v, --version" << endl << "     Displays the current version of this software." << endl << endl
/*! \def mVER_USAGE(A,B)
* \brief Prints the version number.
*/
#define mVER_USAGE(A,B) cout << endl << A << " Version: " << B << endl << endl
/*! \def PID
* \brief Contains the PID path.
*/
#define PID "/.pid"
/*! \def PORT
* \brief Supplies the port.
*/
#define PORT "7678"
/*! \def START
* \brief Contains the start path.
*/
#define START "/.start"
// }}}
// {{{ structs
struct bridge
{
  bool bDone;
  int fdIncoming;
  int fdOutgoing;
  int nThrottle;
  size_t unInRecv;
  size_t unInSend;
  size_t unOutRecv;
  size_t unOutSend;
  string strBuffer[2];
  string strLoadBalancer;
  string strPort;
  string strServer;
  string strServiceJunction;
  time_t CActiveTime;
  time_t CEndTime;
  time_t CStartTime;
  Json *ptInfo;
};
struct service
{
  list<bridge *> active;
  list<bridge *> queue;
};
// }}}
// {{{ global variables
static bool gbDaemon = false; //!< Global daemon variable.
static bool gbShutdown = false; //!< Global shutdown variable.
static list<bridge *> loadBridge; //!< Global bridge entry data.
static map<string, service *> services; //!< Global services variable.
static string gstrApplication = "Port Concentrator"; //!< Global application name.
static string gstrData = "/data/portconcentrator"; //!< Global data path.
static string gstrEmail; //!< Global notification email address.
static Central *gpCentral = NULL; //!< Contains the Central class.
mutex mutexLoad; //! < Contains the loadBridge mutex.
// }}}
// {{{ prototypes
/*! \fn void sighandle(const int nSignal)
* \brief Establishes signal handling for the application.
* \param nSignal Contains the caught signal.
*/
void sighandle(const int nSignal);
/*! \fn void active(bridge *ptBridge)
* \brief Bridges the socket communication.
* \param ptBridge Contains the bridge.
*/
void active(bridge *ptBridge);
/*! \fn void *queue(int fdSocket)
* \brief Adds a socket to the queue.
* \param fdSocket Contains socket descriptor.
*/
void queue(int fdSocket);
/*! \fn void throttle()
* \brief Maintains the various socket throttles.
*/
void throttle();
// }}}
// {{{ main()
/*! \fn int main(int argc, char *argv[])
* \brief This is the main function.
* \return Exits with a return code for the operating system.
*/
int main(int argc, char *argv[])
{ 
  string strError;
  
  gpCentral = new Central(strError);
  // {{{ set signal handling
  sethandles(sighandle);
  signal(SIGBUS, SIG_IGN);
  signal(SIGCHLD, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGSEGV, SIG_IGN);
  signal(SIGWINCH, SIG_IGN);
  // }}}
  // {{{ command line arguments
  for (int i = 1; i < argc; i++)
  { 
    string strArg = argv[i];
    if (strArg.size() > 7 && strArg.substr(0, 7) == "--conf=")
    { 
      string strConf;
      if (strArg == "-c" && i + 1 < argc && argv[i+1][0] != '-')
      {
        strConf = argv[++i];
      }
      else
      {
        strConf = strArg.substr(7, strArg.size() - 7);
      }
      gpCentral->manip()->purgeChar(strConf, strConf, "'");
      gpCentral->manip()->purgeChar(strConf, strConf, "\"");
      gpCentral->utility()->setConfPath(strConf, strError);
    } 
    else if (strArg == "-d" || strArg == "--daemon")
    { 
      gbDaemon = true;
    }
    else if (strArg.size() > 7 && strArg.substr(0, 7) == "--data=")
    { 
      gstrData = strArg.substr(7, strArg.size() - 7);
      gpCentral->manip()->purgeChar(gstrData, gstrData, "'");
      gpCentral->manip()->purgeChar(gstrData, gstrData, "\"");
    } 
    else if (strArg == "-e" || (strArg.size() > 8 && strArg.substr(0, 8) == "--email="))
    {   
      if (strArg == "-e" && i + 1 < argc && argv[i+1][0] != '-')
      {
        gstrEmail = argv[++i];
      }
      else
      {
        gstrEmail = strArg.substr(8, strArg.size() - 8);
      }
      gpCentral->manip()->purgeChar(gstrEmail, gstrEmail, "'");
      gpCentral->manip()->purgeChar(gstrEmail, gstrEmail, "\"");
    }
    else if (strArg == "-h" || strArg == "--help")
    {
      mUSAGE(argv[0]);
      return 0;
    }
    else if (strArg == "-v" || strArg == "--version")
    {
      mVER_USAGE(argv[0], VERSION);
      return 0;
    }
    else
    {
      cout << endl << "Illegal option, '" << strArg << "'." << endl;
      mUSAGE(argv[0]);
      return 0;
    }
  }
  // }}}
  gpCentral->setApplication(gstrApplication);
  gpCentral->setEmail(gstrEmail);
  gpCentral->setLog(gstrData, "concentrator_", "daily", true, true);
  gpCentral->setRoom("#system");
  // {{{ normal run
  if (!gstrEmail.empty())
  {
    setlocale(LC_ALL, "");
    SSL_library_init();
    OpenSSL_add_all_algorithms();
    SSL_load_error_strings();
    //if (gpCentral->utility()->isProcessAlreadyRunning("concentrator"))
    //{
    //  gbShutdown = true;
    //}
    if (!gbShutdown)
    {
      int nReturn;
      struct addrinfo hints, *result;
      if (gbDaemon)
      {
        gpCentral->utility()->daemonize();
      }
      ofstream outPid((gstrData + PID).c_str());
      if (outPid.good())
      {
        outPid << getpid() << endl;
      }
      outPid.close();
      ofstream outStart((gstrData + START).c_str());
      outStart.close();
      thread tThread(throttle);
      pthread_setname_np(tThread.native_handle(), "throttle");
      tThread.detach();
      memset(&hints, 0, sizeof(struct addrinfo));
      hints.ai_family = AF_INET6;
      hints.ai_socktype = SOCK_STREAM;
      hints.ai_flags = AI_PASSIVE;
      if ((nReturn = getaddrinfo(NULL, PORT, &hints, &result)) == 0)
      {
        bool bBound = false, bSocket = false;
        int fdSocket;
        struct addrinfo *rp;
        for (rp = result; !bBound && rp != NULL; rp = rp->ai_next)
        {
          if ((fdSocket = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol)) >= 0)
          {
            int nOn = 1;
            bSocket = true;
            setsockopt(fdSocket, SOL_SOCKET, SO_REUSEADDR, (char *)&nOn, sizeof(nOn));
            if (bind(fdSocket, rp->ai_addr, rp->ai_addrlen) == 0)
            {
              bBound = true;
            }
            else
            {
              close(fdSocket);
            }
          }
        }
        freeaddrinfo(result);
        if (bBound)
        {
          gpCentral->log((string)"Bound to the socket.", strError);
          if (listen(fdSocket, SOMAXCONN) == 0)
          {
            int fdIncoming;
            socklen_t clilen;
            sockaddr_in cli_addr;
            gpCentral->log((string)"Listening to the socket.", strError);
            clilen = sizeof(cli_addr);
            while ((fdIncoming = accept(fdSocket, (struct sockaddr *)&cli_addr, &clilen)) >= 0)
            {
              thread tThread(queue, fdIncoming);
              pthread_setname_np(tThread.native_handle(), "queue");
              tThread.detach();
            }
          }
          else
          {
            gpCentral->alert((string)"listen() error:  " + (string)strerror(errno), strError);
          }
          close(fdSocket);
        }
        else if (!bSocket)
        {
          gpCentral->alert((string)"socket() error:  " + (string)strerror(errno), strError);
        }
        else
        {
          gpCentral->alert((string)"bind() error:  " + (string)strerror(errno), strError);
        }
      }
      else
      {
        gpCentral->alert((string)"getaddrinfo():  " + (string)gai_strerror(nReturn), strError);
      }
      // {{{ check pid file
      if (gpCentral->file()->fileExist((gstrData + PID).c_str()))
      {
        gpCentral->file()->remove((gstrData + PID).c_str());
      }
      // }}}
    }
  }
  // }}}
  // {{{ usage statement
  else
  {
    mUSAGE(argv[0]);
  }
  // }}}
  delete gpCentral;

  return 0;
}
// }}}
// {{{ active()
void active(bridge *ptBridge)
{
  bool bAddrInfo = false, bConnected = false, bSocket = false;
  int nReturn;
  list<string> serverGroup;
  string strError;
  stringstream ssMessage;

  if (!ptBridge->strServer.empty())
  {
    serverGroup.push_back(ptBridge->strServer);
  }
  else
  {
    if (!ptBridge->strLoadBalancer.empty())
    {
      serverGroup.push_back(ptBridge->strLoadBalancer);
    }
    if (!ptBridge->strServiceJunction.empty())
    {
      serverGroup.push_back(ptBridge->strServiceJunction);
    }
  }
  if (!serverGroup.empty())
  {
    for (auto i = serverGroup.begin(); !bConnected && i != serverGroup.end(); i++)
    {
      string strServer;
      timeval tTimeVal;
      unsigned int unAttempt = 0, unPick = 0, unSeed = time(NULL);
      vector<string> server;
      tTimeVal.tv_sec = 2;
      tTimeVal.tv_usec = 0;
      for (int j = 1; !gpCentral->manip()->getToken(strServer, (*i), j, ",", true).empty(); j++)
      {
        server.push_back(gpCentral->manip()->trim(strServer, strServer));
      }
      srand(unSeed);
      unPick = rand_r(&unSeed) % server.size();
      bAddrInfo = bSocket = false;
      while (!bConnected && unAttempt++ < server.size())
      {
        struct addrinfo hints, *result;
        if (unPick == server.size())
        {
          unPick = 0;
        }
        strServer = server[unPick];
        memset(&hints, 0, sizeof(struct addrinfo));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = 0;
        hints.ai_protocol = 0;
        bAddrInfo = bSocket = false;
        if ((nReturn = getaddrinfo(strServer.c_str(), ptBridge->strPort.c_str(), &hints, &result)) == 0)
        {
          struct addrinfo *rp;
          bAddrInfo = true;
          for (rp = result; !bConnected && rp != NULL; rp = rp->ai_next)
          {
            if ((ptBridge->fdOutgoing = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol)) >= 0)
            {
              bSocket = true;
              setsockopt(ptBridge->fdOutgoing, SOL_SOCKET, SO_RCVTIMEO, &tTimeVal, sizeof(timeval));
              if (connect(ptBridge->fdOutgoing, rp->ai_addr, rp->ai_addrlen) == 0)
              {
                bConnected = true;
                ptBridge->strServer = strServer;
              }
              else
              {
                close(ptBridge->fdOutgoing);
              }
            }
          }
          freeaddrinfo(result);
        }
        unPick++;
      }
    }
    serverGroup.clear();
  }
  if (bConnected)
  {
    bool bExit = false;
    char szBuffer[65536];
    int nReturn;
    pollfd *fds;
    size_t unIndex;
    time_t CTime[2];
    time(&(CTime[0]));
    while (!bExit)
    {
      unIndex = 0;
      if (ptBridge->fdIncoming != -1)
      {
        unIndex++;
      }
      if (ptBridge->fdOutgoing != -1)
      {
        unIndex++;
      }
      if (unIndex > 0)
      {
        fds = new pollfd[2];
        unIndex = 0;
        fds[unIndex].fd = ptBridge->fdIncoming;
        fds[unIndex].events = POLLIN;
        if (!ptBridge->strBuffer[0].empty())
        {
          fds[unIndex].events |= POLLOUT;
        }
        unIndex++;
        fds[unIndex].fd = ptBridge->fdOutgoing;
        fds[unIndex].events = POLLIN;
        if (!ptBridge->strBuffer[1].empty())
        {
          fds[unIndex].events |= POLLOUT;
        }
        unIndex++;
        if ((nReturn = poll(fds, unIndex, 250)) > 0)
        {
          for (size_t i = 0; i < unIndex; i++)
          {
            bool bIn = false;
            if (fds[i].fd == ptBridge->fdIncoming)
            {
              bIn = true;
            }
            if (fds[i].revents & POLLIN)
            {
              if ((nReturn = read(fds[i].fd, szBuffer, 65536)) > 0)
              {
                if (bIn)
                {
                  ptBridge->unInRecv += nReturn;
                }
                else
                {
                  ptBridge->unOutRecv += nReturn;
                }
                ptBridge->strBuffer[((bIn)?1:0)].append(szBuffer, nReturn);
              }
              else
              {
                bExit = true;
                if (nReturn < 0)
                {
                  ssMessage.str("");
                  ssMessage << "active()->read(" << errno << ") error:  " << strerror(errno);
                  gpCentral->log(ssMessage.str());
                }
              }
            }
            if (fds[i].revents & POLLOUT)
            {
              if ((nReturn = write(fds[i].fd, ptBridge->strBuffer[((bIn)?0:1)].c_str(), ptBridge->strBuffer[((bIn)?0:1)].size())) > 0)
              {
                if (bIn)
                {
                  ptBridge->unInSend += nReturn;
                }
                else
                {
                  ptBridge->unOutSend += nReturn;
                }
                ptBridge->strBuffer[((bIn)?0:1)].erase(0, nReturn);
              }
              else
              {
                bExit= true;
                if (nReturn < 0)
                {
                  ssMessage.str("");
                  ssMessage << "active()->write(" << errno << ") error:  " << strerror(errno);
                  gpCentral->log(ssMessage.str());
                }
              }
            }
          }
        }
        else if (nReturn < 0)
        {
          bExit = true;
          ptBridge->ptInfo->insert("Error", (string)"poll() error:  " + (string)strerror(errno));
        }
        delete[] fds;
      }
      else
      {
        bExit = true;
      }
      time(&(CTime[1]));
      if ((CTime[1] - CTime[0]) > 600)
      {
        bExit = true;
        ptBridge->ptInfo->insert("Error", "error:  Exceeded 10 minute timeout.");
      }
    }
    close(ptBridge->fdOutgoing);
  }
  else
  {
    stringstream ssMessage;
    if (!bAddrInfo)
    {
      ssMessage << "getaddrinfo() error:  " << gai_strerror(nReturn);
    }
    else
    {
      if (!bSocket)
      {
        ssMessage << "socket()";
      }
      else
      {
        ssMessage << "connect()";
      }
      ssMessage << ":  " << strerror(errno);
    }
    ptBridge->ptInfo->insert("Error", ssMessage.str());
  }
  close(ptBridge->fdIncoming);
  ptBridge->bDone = true;
}
// }}}
// {{{ queue()
void queue(int fdSocket)
{
  bool bValid = false;
  char szIP[INET6_ADDRSTRLEN];
  sockaddr_storage addr;
  socklen_t len = sizeof(addr);
  string strRequest;

  getpeername(fdSocket, (sockaddr*)&addr, &len);
  if (addr.ss_family == AF_INET)
  {
    sockaddr_in *s = (sockaddr_in *)&addr;
    inet_ntop(AF_INET, &s->sin_addr, szIP, sizeof(szIP));
  }
  else if (addr.ss_family == AF_INET6)
  {
    sockaddr_in6 *s = (sockaddr_in6 *)&addr;
    inet_ntop(AF_INET6, &s->sin6_addr, szIP, sizeof(szIP));
  }
  if (gpCentral->utility()->getLine(fdSocket, strRequest))
  {
    map<string, string> request;
    Json *ptConf = gpCentral->utility()->conf(), *ptJson = new Json(strRequest);
    ptJson->flatten(request, true, false);
    delete ptJson;
    if (request.find("Service") != request.end() && !request["Service"].empty() && request.find("Throttle") != request.end() && !request["Throttle"].empty() && atoi(request["Throttle"].c_str()) > 0 && (request.find("Server") == request.end() || request["Server"].empty() || (request.find("Port") != request.end() && !request["Port"].empty())))
    {
      bridge *ptBridge = new bridge;
      bValid = true;
      ptBridge->bDone = false;
      ptBridge->unInRecv = 0;
      ptBridge->unInSend = 0;
      ptBridge->unOutRecv = 0;
      ptBridge->unOutSend = 0;
      ptBridge->ptInfo = new Json(request);
      if (request.find("Server") != request.end() && !request["Server"].empty())
      {
        ptBridge->strServer = request["Server"];
        ptBridge->strPort = request["Port"];
      }
      else
      {
        if (ptConf->m.find("Load Balancer") != ptConf->m.end() && !ptConf->m["Load Balancer"]->v.empty())
        {
          ptBridge->strLoadBalancer = ptConf->m["Load Balancer"]->v;
        }
        if (ptConf->m.find("Service Junction") != ptConf->m.end() && !ptConf->m["Service Junction"]->v.empty())
        {
          ptBridge->strServiceJunction = ptConf->m["Service Junction"]->v;
        }
        ptBridge->strPort = "5864";
      }
      ptBridge->ptInfo->insert("IP", szIP);
      ptBridge->nThrottle = atoi(request["Throttle"].c_str());
      ptBridge->fdIncoming = fdSocket;
      ptBridge->fdOutgoing = -1;
      time(&(ptBridge->CStartTime));
      mutexLoad.lock();
      loadBridge.push_back(ptBridge);
      mutexLoad.unlock();
    }
    request.clear();
  }
  if (!bValid)
  {
    close(fdSocket);
  }
}
// }}}
// {{{ sighandle()
void sighandle(const int nSignal)
{
  string strError, strSignal;
  stringstream ssSignal;

  sethandles(sigdummy);
  gbShutdown = true;
  if (nSignal != SIGINT && nSignal != SIGTERM)
  {
    ssSignal << nSignal;
    gpCentral->notify("", (string)"The program's signal handling caught a " + (string)sigstring(strSignal, nSignal) + (string)"(" + ssSignal.str() + (string)")!  Exiting...", strError);
  }
  exit(1);
}
// }}}
// {{{ throttle()
void throttle()
{
  string strError;

  while (!gbShutdown)
  {
    bool bUpdated = false;
    list<map<string, service *>::iterator> removeService;
    mutexLoad.lock();
    while (!loadBridge.empty())
    {
      bridge *ptBridge = loadBridge.front();
      if (services.find(ptBridge->ptInfo->m["Service"]->v) == services.end())
      {
        service *ptService = new service;
        services[ptBridge->ptInfo->m["Service"]->v] = ptService;
      }
      if (ptBridge->ptInfo->m.find("Load") != ptBridge->ptInfo->m.end())
      {
        delete ptBridge->ptInfo->m["Load"];
      }
      ptBridge->ptInfo->m["Load"] = new Json;
      if (ptBridge->ptInfo->m.find("Transfer") != ptBridge->ptInfo->m.end())
      {
        delete ptBridge->ptInfo->m["Transfer"];
      }
      ptBridge->ptInfo->m["Transfer"] = new Json;
      ptBridge->ptInfo->m["Transfer"]->m["In"] = new Json;
      ptBridge->ptInfo->m["Transfer"]->m["Out"] = new Json;
      services[ptBridge->ptInfo->m["Service"]->v]->queue.push_back(ptBridge);
      loadBridge.pop_front();
    }
    mutexLoad.unlock();
    for (auto i = services.begin(); i != services.end(); i++)
    {
      list<list<bridge *>::iterator> removeActive, removeQueue;
      size_t nActive = i->second->active.size(), nQueue = i->second->queue.size();
      for (auto j = i->second->active.begin(); j != i->second->active.end(); j++)
      {
        if ((*j)->bDone)
        {
          stringstream ssActive, ssDurationActive, ssDurationQueue, ssInRecv, ssInSend, ssMessage, ssOutRecv, ssOutSend, ssQueue;
          ssActive << (nActive - removeActive.size() - 1);
          (*j)->ptInfo->m["Load"]->insert("Active", ssActive.str(), 'n');
          ssQueue << nQueue;
          (*j)->ptInfo->m["Load"]->insert("Queue", ssQueue.str(), 'n');
          time(&((*j)->CEndTime));
          ssDurationActive << ((*j)->CEndTime - (*j)->CActiveTime);
          (*j)->ptInfo->insert("Duration (active)", ssDurationActive.str(), 'n');
          ssDurationQueue << ((*j)->CActiveTime - (*j)->CStartTime);
          (*j)->ptInfo->insert("Duration (queue)", ssDurationQueue.str(), 'n');
          ssInRecv << (*j)->unInRecv;
          (*j)->ptInfo->m["Transfer"]->m["In"]->insert("Recv", ssInRecv.str(), 'n');
          ssInSend << (*j)->unInSend;
          (*j)->ptInfo->m["Transfer"]->m["In"]->insert("Send", ssInSend.str(), 'n');
          ssOutRecv << (*j)->unOutRecv;
          (*j)->ptInfo->m["Transfer"]->m["Out"]->insert("Recv", ssOutRecv.str(), 'n');
          ssOutSend << (*j)->unOutSend;
          (*j)->ptInfo->m["Transfer"]->m["Out"]->insert("Send", ssOutSend.str(), 'n');
          ssMessage << (*j)->ptInfo;
          if ((*j)->ptInfo->m.find("Error") != (*j)->ptInfo->m.end() && !(*j)->ptInfo->m["Error"]->v.empty())
          {
            ssMessage << ":  " << (*j)->ptInfo->m["Error"]->v;
          }
          gpCentral->log(ssMessage.str(), strError);
          delete (*j)->ptInfo;
          delete (*j);
          removeActive.push_back(j);
        }
      }
      for (auto &j : removeActive)
      {
        i->second->active.erase(j);
      }
      removeActive.clear();
      for (auto j = i->second->queue.begin(); j != i->second->queue.end(); j++)
      {
        if ((int)i->second->active.size() < (*j)->nThrottle)
        {
          bUpdated = true;
          time(&((*j)->CActiveTime));
          i->second->active.push_back(*j);
          thread tThread(active, (*j));
          pthread_setname_np(tThread.native_handle(), "active");
          tThread.detach();
          removeQueue.push_back(j);
        }
      }
      for (auto &j : removeQueue)
      {
        i->second->queue.erase(j);
      }
      removeQueue.clear();
      if (i->second->active.empty() && i->second->queue.empty())
      {
        delete i->second;
        removeService.push_back(i);
      }
    }
    for (auto &i : removeService)
    {
      services.erase(i);
    }
    removeService.clear();
    if (!bUpdated)
    {
      gpCentral->utility()->msleep(250);
    }
  }
}
// }}}
