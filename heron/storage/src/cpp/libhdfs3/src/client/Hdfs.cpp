/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "platform.h"

#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileSystem.h"
#include "hdfs.h"
#include "InputStream.h"
#include "Logger.h"
#include "Logger.h"
#include "Memory.h"
#include "OutputStream.h"
#include "server/NamenodeInfo.h"
#include "SessionConfig.h"
#include "Thread.h"
#include "XmlConfig.h"

#include <vector>
#include <string>
#include <libxml/uri.h>

#ifdef __cplusplus
extern "C" {
#endif

#define KERBEROS_TICKET_CACHE_PATH "hadoop.security.kerberos.ticket.cache.path"

#ifndef ERROR_MESSAGE_BUFFER_SIZE
#define ERROR_MESSAGE_BUFFER_SIZE 4096
#endif

static THREAD_LOCAL char ErrorMessage[ERROR_MESSAGE_BUFFER_SIZE] = "Success";

static void SetLastException(Hdfs::exception_ptr e) {
    std::string buffer;
    const char *p;
    p = Hdfs::Internal::GetExceptionMessage(e, buffer);
    strncpy(ErrorMessage, p, sizeof(ErrorMessage) - 1);
    ErrorMessage[sizeof(ErrorMessage) - 1] = 0;
}

static void SetErrorMessage(const char *msg) {
    assert(NULL != msg);
    strncpy(ErrorMessage, msg, sizeof(ErrorMessage) - 1);
    ErrorMessage[sizeof(ErrorMessage) - 1] = 0;
}

#define PARAMETER_ASSERT(para, retval, eno) \
    if (!(para)) { \
        SetErrorMessage(Hdfs::Internal::GetSystemErrorInfo(eno)); \
        errno = eno; \
        return retval; \
    }

static inline char * Strdup(const char * str) {
    if (str == NULL) {
        return NULL;
    }

    int len = strlen(str);
    char * retval = new char[len + 1];
    memcpy(retval, str, len + 1);
    return retval;
}

using Hdfs::InputStream;
using Hdfs::OutputStream;
using Hdfs::FileSystem;
using Hdfs::exception_ptr;
using Hdfs::Config;
using Hdfs::Internal::shared_ptr;
using Hdfs::NamenodeInfo;
using Hdfs::FileNotFoundException;

struct HdfsFileInternalWrapper {
public:
    HdfsFileInternalWrapper() :
        input(true), stream(NULL) {
    }

    ~HdfsFileInternalWrapper() {
        if (input) {
            delete static_cast<InputStream *>(stream);
        } else {
            delete static_cast<OutputStream *>(stream);
        }
    }

    InputStream & getInputStream() {
        if (!input) {
            THROW(Hdfs::HdfsException,
                  "Internal error: file was not opened for read.");
        }

        if (!stream) {
            THROW(Hdfs::HdfsIOException, "File is not opened.");
        }

        return *static_cast<InputStream *>(stream);
    }
    OutputStream & getOutputStream() {
        if (input) {
            THROW(Hdfs::HdfsException,
                  "Internal error: file was not opened for write.");
        }

        if (!stream) {
            THROW(Hdfs::HdfsIOException, "File is not opened.");
        }

        return *static_cast<OutputStream *>(stream);
    }

    bool isInput() const {
        return input;
    }

    void setInput(bool input) {
        this->input = input;
    }

    void setStream(void * stream) {
        this->stream = stream;
    }

private:
    bool input;
    void * stream;
};

struct HdfsFileSystemInternalWrapper {
public:
    HdfsFileSystemInternalWrapper(FileSystem * fs) :
        filesystem(fs) {
    }

    ~HdfsFileSystemInternalWrapper() {
        delete filesystem;
    }

    FileSystem & getFilesystem() {
        return *filesystem;
    }

private:
    FileSystem * filesystem;
};

class DefaultConfig {
public:
    DefaultConfig() : conf(new Hdfs::Config) {
        bool reportError = false;
        const char * env = getenv("LIBHDFS3_CONF");
        std::string confPath = env ? env : "";

        if (!confPath.empty()) {
            size_t pos = confPath.find_first_of('=');

            if (pos != confPath.npos) {
                confPath = confPath.c_str() + pos + 1;
            }

            reportError = true;
        } else {
            confPath = "hdfs-client.xml";
        }

        init(confPath, reportError);
    }

    DefaultConfig(const char * path) : conf(new Hdfs::Config) {
        assert(path != NULL && strlen(path) > 0);
        init(path, true);
    }

    shared_ptr<Config> getConfig() {
        return conf;
    }

private:
    void init(const std::string & confPath, bool reportError) {
        if (access(confPath.c_str(), R_OK)) {
            if (reportError) {
                LOG(Hdfs::Internal::LOG_ERROR,
                    "Environment variable LIBHDFS3_CONF is set but %s cannot be read",
                    confPath.c_str());
            } else {
                return;
            }
        }

        conf->update(confPath.c_str());
    }
private:
    shared_ptr<Config> conf;
};

struct hdfsBuilder {
public:
    hdfsBuilder() :
        conf(DefaultConfig().getConfig()), port(0) {
    }

    ~hdfsBuilder() {
    }

public:
    std::string token;
    shared_ptr<Config> conf;
    std::string nn;
    std::string userName;
    tPort port;
};

static void handleException(Hdfs::exception_ptr error) {
    try {
        Hdfs::rethrow_exception(error);

#ifndef NDEBUG
        std::string buffer;
        LOG(Hdfs::Internal::LOG_ERROR, "Handle Exception: %s",
            Hdfs::Internal::GetExceptionDetail(error, buffer));
#endif
    } catch (Hdfs::AccessControlException &) {
        errno = EACCES;
    } catch (Hdfs::AlreadyBeingCreatedException &) {
        errno = EBUSY;
    } catch (Hdfs::ChecksumException &) {
        errno = EIO;
    } catch (Hdfs::DSQuotaExceededException &) {
        errno = ENOSPC;
    } catch (Hdfs::FileAlreadyExistsException &) {
        errno = EEXIST;
    } catch (Hdfs::FileNotFoundException &) {
        errno = ENOENT;
    } catch (const Hdfs::HdfsBadBoolFoumat &) {
        errno = EINVAL;
    } catch (const Hdfs::HdfsBadConfigFoumat &) {
        errno = EINVAL;
    } catch (const Hdfs::HdfsBadNumFoumat &) {
        errno = EINVAL;
    } catch (const Hdfs::HdfsCanceled &) {
        errno = EIO;
    } catch (const Hdfs::HdfsConfigInvalid &) {
        errno = EINVAL;
    } catch (const Hdfs::HdfsConfigNotFound &) {
        errno = EINVAL;
    } catch (const Hdfs::HdfsEndOfStream &) {
        errno = EOVERFLOW;
    } catch (const Hdfs::HdfsInvalidBlockToken &) {
        errno = EPERM;
    } catch (const Hdfs::HdfsTimeoutException &) {
        errno = EIO;
    } catch (Hdfs::HadoopIllegalArgumentException &) {
        errno = EINVAL;
    } catch (Hdfs::InvalidParameter &) {
        errno = EINVAL;
    } catch (Hdfs::InvalidPath &) {
        errno = EINVAL;
    } catch (Hdfs::NotReplicatedYetException &) {
        errno = EINVAL;
    } catch (Hdfs::NSQuotaExceededException &) {
        errno = EINVAL;
    } catch (Hdfs::ParentNotDirectoryException &) {
        errno = EACCES;
    } catch (Hdfs::ReplicaNotFoundException &) {
        errno = EACCES;
    } catch (Hdfs::SafeModeException &) {
        errno = EIO;
    } catch (Hdfs::UnresolvedLinkException &) {
        errno = EACCES;
    } catch (Hdfs::HdfsRpcException &) {
        errno = EIO;
    } catch (Hdfs::HdfsNetworkException &) {
        errno = EIO;
    } catch (Hdfs::RpcNoSuchMethodException &) {
        errno = ENOTSUP;
    } catch (Hdfs::UnsupportedOperationException &) {
        errno = ENOTSUP;
    } catch (Hdfs::SaslException &) {
        errno = EACCES;
    } catch (Hdfs::NameNodeStandbyException &) {
        errno = EIO;
    } catch (Hdfs::RecoveryInProgressException &){
        errno = EBUSY;
    } catch (Hdfs::HdfsIOException &) {
        std::string buffer;
        LOG(Hdfs::Internal::LOG_ERROR, "Handle Exception: %s", Hdfs::Internal::GetExceptionDetail(error, buffer));
        errno = EIO;
    } catch (Hdfs::HdfsException & e) {
        std::string buffer;
        LOG(Hdfs::Internal::LOG_ERROR, "Unexpected exception %s: %s", typeid(e).name(),
            Hdfs::Internal::GetExceptionDetail(e, buffer));
        errno = EINTERNAL;
    } catch (std::exception & e) {
        LOG(Hdfs::Internal::LOG_ERROR, "Unexpected exception %s: %s", typeid(e).name(), e.what());
        errno = EINTERNAL;
    }
}

const char * hdfsGetLastError() {
    return ErrorMessage;
}

int hdfsFileIsOpenForRead(hdfsFile file) {
    PARAMETER_ASSERT(file, 0, EINVAL);
    return file->isInput() ? 1 : 0;
}

int hdfsFileIsOpenForWrite(hdfsFile file) {
    PARAMETER_ASSERT(file, 0, EINVAL);
    return !file->isInput() ? 1 : 0;
}

hdfsFS hdfsConnectAsUser(const char * host, tPort port, const char * user) {
    hdfsFS retVal = NULL;
    PARAMETER_ASSERT(host != NULL && strlen(host) > 0, NULL, EINVAL);
    PARAMETER_ASSERT(port > 0, NULL, EINVAL);
    PARAMETER_ASSERT(user != NULL && strlen(user) > 0, NULL, EINVAL);
    struct hdfsBuilder * bld = hdfsNewBuilder();

    if (!bld)
        return NULL;

    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderSetUserName(bld, user);
    retVal = hdfsBuilderConnect(bld);
    hdfsFreeBuilder(bld);
    return retVal;
}

hdfsFS hdfsConnect(const char * host, tPort port) {
    hdfsFS retVal = NULL;
    PARAMETER_ASSERT(host != NULL && strlen(host) > 0, NULL, EINVAL);
    PARAMETER_ASSERT(port > 0, NULL, EINVAL);
    struct hdfsBuilder * bld = hdfsNewBuilder();

    if (!bld)
        return NULL;

    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    retVal = hdfsBuilderConnect(bld);
    hdfsFreeBuilder(bld);
    return retVal;
}

hdfsFS hdfsConnectAsUserNewInstance(const char * host, tPort port,
                                    const char * user) {
    hdfsFS retVal = NULL;
    PARAMETER_ASSERT(host != NULL && strlen(host) > 0, NULL, EINVAL);
    PARAMETER_ASSERT(port > 0, NULL, EINVAL);
    PARAMETER_ASSERT(user != NULL && strlen(user) > 0, NULL, EINVAL);
    struct hdfsBuilder * bld = hdfsNewBuilder();

    if (!bld)
        return NULL;

    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderSetForceNewInstance(bld);
    hdfsBuilderSetUserName(bld, user);
    retVal = hdfsBuilderConnect(bld);
    hdfsFreeBuilder(bld);
    return retVal;
}

hdfsFS hdfsConnectNewInstance(const char * host, tPort port) {
    hdfsFS retVal = NULL;
    PARAMETER_ASSERT(host != NULL && strlen(host) > 0, NULL, EINVAL);
    PARAMETER_ASSERT(port > 0, NULL, EINVAL);
    struct hdfsBuilder * bld = hdfsNewBuilder();

    if (!bld)
        return NULL;

    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderSetForceNewInstance(bld);
    retVal = hdfsBuilderConnect(bld);
    hdfsFreeBuilder(bld);
    return retVal;
}

hdfsFS hdfsBuilderConnect(struct hdfsBuilder * bld) {
    PARAMETER_ASSERT(bld && !bld->nn.empty(), NULL, EINVAL);
    Hdfs::Internal::SessionConfig conf(*bld->conf);
    std::string uri;
    std::stringstream ss;
    ss.imbue(std::locale::classic());
    xmlURIPtr uriobj;
    FileSystem * fs = NULL;

    if (0 == strcasecmp(bld->nn.c_str(), "default")) {
        uri = conf.getDefaultUri();
    } else {
        /*
         * handle scheme
         */
        if (bld->nn.find("://") == bld->nn.npos) {
            uri  = "hdfs://";
        }

        uri += bld->nn;
    }

    uriobj = xmlParseURI(uri.c_str());

    try {
        if (!uriobj) {
            THROW(Hdfs::InvalidParameter, "Cannot parse connection URI");
        }

        if (uriobj->port != 0 && bld->port != 0) {
            THROW(Hdfs::InvalidParameter, "Cannot determinate port");
        }

        if (uriobj->user && !bld->userName.empty()) {
            THROW(Hdfs::InvalidParameter, "Cannot determinate user name");
        }

        ss << uriobj->scheme << "://";

        if (uriobj->user || !bld->userName.empty()) {
            ss << (uriobj->user ? uriobj->user : bld->userName.c_str())
               << '@';
        }

        if (bld->port == 0 && uriobj->port == 0) {
            ss << uriobj->server;
        } else {
            ss << uriobj->server << ":" << (uriobj->port ? uriobj->port : bld->port);
        }

        uri = ss.str();
    } catch (const std::bad_alloc & e) {
        if (uriobj) {
            xmlFreeURI(uriobj);
        }

        SetErrorMessage("Out of memory");
        errno = ENOMEM;
        return NULL;
    } catch (...) {
        if (uriobj) {
            xmlFreeURI(uriobj);
        }

        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
        return NULL;
    }

    xmlFreeURI(uriobj);

    try {
        fs = new FileSystem(*bld->conf);

        if (!bld->token.empty()) {
            fs->connect(uri.c_str(), NULL, bld->token.c_str());
        } else {
            fs->connect(uri.c_str());
        }

        return new HdfsFileSystemInternalWrapper(fs);
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        delete fs;
        errno = ENOMEM;
    } catch (...) {
        delete fs;
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return NULL;
}

struct hdfsBuilder * hdfsNewBuilder(void) {
    try {
        return new struct hdfsBuilder;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return NULL;
}

void hdfsFreeBuilder(struct hdfsBuilder * bld) {
    delete bld;
}

void hdfsBuilderSetForceNewInstance(struct hdfsBuilder * bld) {
    assert(bld);
}

void hdfsBuilderSetNameNode(struct hdfsBuilder * bld, const char * nn) {
    assert(bld != NULL && nn != NULL);
    bld->nn = nn;
}

void hdfsBuilderSetNameNodePort(struct hdfsBuilder * bld, tPort port) {
    assert(bld != NULL && port > 0);
    bld->port = port;
}

void hdfsBuilderSetUserName(struct hdfsBuilder * bld, const char * userName) {
    assert(bld && userName && strlen(userName) > 0);
    bld->userName = userName;
}

void hdfsBuilderSetKerbTicketCachePath(struct hdfsBuilder * bld,
                                       const char * kerbTicketCachePath) {
    assert(bld && kerbTicketCachePath && strlen(kerbTicketCachePath) > 0);
    hdfsBuilderConfSetStr(bld, KERBEROS_TICKET_CACHE_PATH, kerbTicketCachePath);
}

void hdfsBuilderSetToken(struct hdfsBuilder * bld, const char * token) {
    assert(bld && token && strlen(token) > 0 && bld->userName.empty());

    try {
        bld->token = token;
    } catch (const std::bad_alloc & e) {
        errno = ENOMEM;
    } catch (...) {
        handleException(Hdfs::current_exception());
    }
}

int hdfsBuilderConfSetStr(struct hdfsBuilder * bld, const char * key,
                          const char * val) {
    PARAMETER_ASSERT(bld && key && strlen(key) > 0, -1, EINVAL);
    PARAMETER_ASSERT(val && strlen(val) > 0, -1, EINVAL);

    try {
        bld->conf->set(key, val);
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsConfGetStr(const char * key, char ** val) {
    PARAMETER_ASSERT(key && strlen(key) > 0 && val, -1, EINVAL);

    try {
        std::string retval = DefaultConfig().getConfig()->getString(key);
        *val = Strdup(retval.c_str());
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

void hdfsConfStrFree(char * val) {
    delete[] val;
}

int hdfsConfGetInt(const char * key, int32_t * val) {
    PARAMETER_ASSERT(key && strlen(key) > 0 && val, -1, EINVAL);

    try {
        *val = DefaultConfig().getConfig()->getInt32(key);
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsDisconnect(hdfsFS fs) {
    try {
        if (fs) {
            fs->getFilesystem().disconnect();
            delete fs;
        }

        return 0;
    } catch (const std::bad_alloc & e) {
        delete fs;
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        delete fs;
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

hdfsFile hdfsOpenFile(hdfsFS fs, const char * path, int flags, int bufferSize,
                      short replication, tOffset blocksize) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0, NULL, EINVAL);
    PARAMETER_ASSERT(bufferSize >= 0 && replication >= 0 && blocksize >= 0, NULL, EINVAL);
    PARAMETER_ASSERT(!(flags & O_RDWR) && !((flags & O_EXCL) && (flags & O_CREAT)), NULL, ENOTSUP);
    HdfsFileInternalWrapper * file = NULL;
    OutputStream * os = NULL;
    InputStream * is = NULL;

    try {
        file = new HdfsFileInternalWrapper();

        if ((flags & O_CREAT) || (flags & O_APPEND) || (flags & O_WRONLY)) {
            int internalFlags = 0;

            if (flags & O_CREAT)  {
                internalFlags |= Hdfs::Create;
            } else if ((flags & O_APPEND) && (flags & O_WRONLY)) {
                internalFlags |= Hdfs::Create;
                internalFlags |= Hdfs::Append;
            } else if (flags & O_WRONLY) {
                internalFlags |= Hdfs::Create;
                internalFlags |= Hdfs::Overwrite;
            }

            if (flags & O_SYNC) {
                internalFlags |= Hdfs::SyncBlock;
            }

            file->setInput(false);
            os = new OutputStream;
            os->open(fs->getFilesystem(), path, internalFlags, 0777, false, replication,
                     blocksize);
            file->setStream(os);
        } else {
            file->setInput(true);
            is = new InputStream;
            is->open(fs->getFilesystem(), path, true);
            file->setStream(is);
        }

        return file;
    } catch (const std::bad_alloc & e) {
        delete file;
        delete os;
        delete is;
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        delete file;
        delete os;
        delete is;
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return NULL;
}

int hdfsCloseFile(hdfsFS fs, hdfsFile file) {
    PARAMETER_ASSERT(fs, -1, EINVAL);

    try {
        if (file) {
            if (file->isInput()) {
                file->getInputStream().close();
            } else {
                file->getOutputStream().close();
            }

            delete file;
        }

        return 0;
    } catch (const std::bad_alloc & e) {
        delete file;
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        delete file;
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsExists(hdfsFS fs, const char * path) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0, -1, EINVAL);

    try {
        return fs->getFilesystem().exist(path) ? 0 : -1;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos) {
    PARAMETER_ASSERT(fs && file && desiredPos >= 0, -1, EINVAL);
    PARAMETER_ASSERT(file->isInput(), -1, EINVAL);

    try {
        file->getInputStream().seek(desiredPos);
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

tOffset hdfsTell(hdfsFS fs, hdfsFile file) {
    PARAMETER_ASSERT(fs && file, -1, EINVAL);

    try {
        if (file->isInput()) {
            return file->getInputStream().tell();
        } else {
            return file->getOutputStream().tell();
        }
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

tSize hdfsRead(hdfsFS fs, hdfsFile file, void * buffer, tSize length) {
    PARAMETER_ASSERT(fs && file && buffer && length > 0, -1, EINVAL);
    PARAMETER_ASSERT(file->isInput(), -1, EINVAL);

    try {
        return file->getInputStream().read(static_cast<char *>(buffer), length);
    } catch (const Hdfs::HdfsEndOfStream & e) {
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void * buffer, tSize length) {
    PARAMETER_ASSERT(fs && file && buffer && length > 0, -1, EINVAL);
    PARAMETER_ASSERT(!file->isInput(), -1, EINVAL);

    try {
        file->getOutputStream().append(static_cast<const char *>(buffer),
                                       length);
        return length;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsFlush(hdfsFS fs, hdfsFile file) {
    PARAMETER_ASSERT(fs && file && file, -1, EINVAL);
    return hdfsHFlush(fs, file);
}

int hdfsHFlush(hdfsFS fs, hdfsFile file) {
    PARAMETER_ASSERT(fs && file && file, -1, EINVAL);
    PARAMETER_ASSERT(!file->isInput(), -1, EINVAL);

    try {
        file->getOutputStream().flush();
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsSync(hdfsFS fs, hdfsFile file) {
    PARAMETER_ASSERT(fs && file && file, -1, EINVAL);
    PARAMETER_ASSERT(!file->isInput(), -1, EINVAL);

    try {
        file->getOutputStream().sync();
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsAvailable(hdfsFS fs, hdfsFile file) {
    PARAMETER_ASSERT(fs && file && file, -1, EINVAL);
    PARAMETER_ASSERT(file->isInput(), -1, EINVAL);

    try {
        int max = std::numeric_limits<int>::max();
        int64_t retval = file->getInputStream().available();
        return retval < max ? retval : max;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsCopy(hdfsFS srcFS, const char *src, hdfsFS dstFS, const char *dst) {
    PARAMETER_ASSERT(srcFS && dstFS, -1, EINVAL);
    PARAMETER_ASSERT(src && strlen(src) > 0, -1, EINVAL);
    PARAMETER_ASSERT(dst && strlen(dst) > 0, -1, EINVAL);

    errno = ENOTSUP;
    return -1;
}

int hdfsMove(hdfsFS srcFS, const char *src, hdfsFS dstFS, const char *dst) {
    PARAMETER_ASSERT(srcFS && dstFS, -1, EINVAL);
    PARAMETER_ASSERT(src && strlen(src) > 0, -1, EINVAL);
    PARAMETER_ASSERT(dst && strlen(dst) > 0, -1, EINVAL);

    errno = ENOTSUP;
    return -1;
}

int hdfsDelete(hdfsFS fs, const char * path, int recursive) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0, -1, EINVAL);

    try {
        return fs->getFilesystem().deletePath(path, recursive) ? 0 : -1;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsRename(hdfsFS fs, const char * oldPath, const char * newPath) {
    PARAMETER_ASSERT(fs && oldPath && strlen(oldPath) > 0, -1, EINVAL);
    PARAMETER_ASSERT(newPath && strlen(newPath) > 0, -1, EINVAL);

    try {
        return fs->getFilesystem().rename(oldPath, newPath) ? 0 : -1;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

char * hdfsGetWorkingDirectory(hdfsFS fs, char * buffer, size_t bufferSize) {
    PARAMETER_ASSERT(fs && buffer && bufferSize > 0, NULL, EINVAL);

    try {
        std::string retval = fs->getFilesystem().getWorkingDirectory();
        PARAMETER_ASSERT(retval.length() + 1 <= bufferSize, NULL, ENOMEM);
        strncpy(buffer, retval.c_str(), bufferSize);
        return buffer;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return NULL;
}

int hdfsSetWorkingDirectory(hdfsFS fs, const char * path) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0, -1, EINVAL);

    try {
        fs->getFilesystem().setWorkingDirectory(path);
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsCreateDirectory(hdfsFS fs, const char * path) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0, -1, EINVAL);

    try {
        return fs->getFilesystem().mkdirs(path, 0755) ? 0 : -1;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsSetReplication(hdfsFS fs, const char * path, int16_t replication) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0 && replication > 0, -1, EINVAL);

    try {
        return fs->getFilesystem().setReplication(path, replication) ? 0 : -1;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

static void ConstructHdfsFileInfo(hdfsFileInfo * infos,
                                  const std::vector<Hdfs::FileStatus> & status) {
    size_t size = status.size();

    for (size_t i = 0; i < size; ++i) {
        infos[i].mBlockSize = status[i].getBlockSize();
        infos[i].mGroup = Strdup(status[i].getGroup());
        infos[i].mKind =
            status[i].isDirectory() ?
            kObjectKindDirectory : kObjectKindFile;
        infos[i].mLastAccess = status[i].getAccessTime() / 1000;
        infos[i].mLastMod = status[i].getModificationTime() / 1000;
        infos[i].mName = Strdup(status[i].getPath());
        infos[i].mOwner = Strdup(status[i].getOwner());
        infos[i].mPermissions = status[i].getPermission().toShort();
        infos[i].mReplication = status[i].getReplication();
        infos[i].mSize = status[i].getLength();
    }
}

hdfsFileInfo * hdfsListDirectory(hdfsFS fs, const char * path,
                                 int * numEntries) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0 && numEntries, NULL, EINVAL);
    hdfsFileInfo * retval = NULL;
    int size = 0;

    try {
        std::vector<Hdfs::FileStatus> status =
            fs->getFilesystem().listAllDirectoryItems(path);
        size = status.size();
        retval = new hdfsFileInfo[size];
        memset(retval, 0, sizeof(hdfsFileInfo) * size);
        ConstructHdfsFileInfo(&retval[0], status);
        *numEntries = size;
        return retval;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        hdfsFreeFileInfo(retval, size);
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        hdfsFreeFileInfo(retval, size);
        handleException(Hdfs::current_exception());
    }

    return NULL;
}

hdfsFileInfo * hdfsGetPathInfo(hdfsFS fs, const char * path) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0, NULL, EINVAL);
    hdfsFileInfo * retval = NULL;

    try {
        retval = new hdfsFileInfo[1];
        memset(retval, 0, sizeof(hdfsFileInfo));
        std::vector<Hdfs::FileStatus> status(1);
        status[0] = fs->getFilesystem().getFileStatus(path);
        ConstructHdfsFileInfo(retval, status);
        return retval;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        hdfsFreeFileInfo(retval, 1);
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        hdfsFreeFileInfo(retval, 1);
        handleException(Hdfs::current_exception());
    }

    return NULL;
}

void hdfsFreeFileInfo(hdfsFileInfo * infos, int numEntries) {
    for (int i = 0; infos != NULL && i < numEntries; ++i) {
        delete [] infos[i].mGroup;
        delete [] infos[i].mName;
        delete [] infos[i].mOwner;
    }

    delete[] infos;
}

char ***hdfsGetHosts(hdfsFS fs, const char *path, tOffset start,
                     tOffset length) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0, NULL, EINVAL);
    PARAMETER_ASSERT(start >= 0 && length > 0, NULL, EINVAL);
    char ***retval = NULL;

    try {
        std::vector<Hdfs::BlockLocation> bls =
            fs->getFilesystem().getFileBlockLocations(path, start, length);
        retval = new char **[bls.size() + 1];
        memset(retval, 0, sizeof(char **) * (bls.size() + 1));

        for (size_t i = 0; i < bls.size(); ++i) {
            const std::vector<std::string> &hosts = bls[i].getHosts();
            retval[i] = new char *[hosts.size() + 1];
            memset(retval[i], 0, sizeof(char *) * (hosts.size() + 1));

            for (size_t j = 0; j < hosts.size(); ++j) {
                retval[i][j] = Strdup(hosts[j].c_str());
            }
        }

        return retval;
    } catch (const std::bad_alloc &e) {
        SetErrorMessage("Out of memory");
        hdfsFreeHosts(retval);
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        hdfsFreeHosts(retval);
        handleException(Hdfs::current_exception());
    }

    return NULL;
}

void hdfsFreeHosts(char ***blockHosts) {
    if (blockHosts == NULL) {
        return;
    }

    for (int i = 0; blockHosts[i] != NULL; ++i) {
        for (int j = 0; blockHosts[i][j] != NULL; ++j) {
            delete[] blockHosts[i][j];
        }

        delete[] blockHosts[i];
    }

    delete[] blockHosts;
}

tOffset hdfsGetDefaultBlockSize(hdfsFS fs) {
    PARAMETER_ASSERT(fs != NULL, -1, EINVAL);

    try {
        return fs->getFilesystem().getDefaultBlockSize();
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

tOffset hdfsGetCapacity(hdfsFS fs) {
    PARAMETER_ASSERT(fs != NULL, -1, EINVAL);

    try {
        Hdfs::FileSystemStats stat = fs->getFilesystem().getStats();
        return stat.getCapacity();
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

tOffset hdfsGetUsed(hdfsFS fs) {
    PARAMETER_ASSERT(fs != NULL, -1, EINVAL);

    try {
        Hdfs::FileSystemStats stat = fs->getFilesystem().getStats();
        return stat.getUsed();
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsChown(hdfsFS fs, const char * path, const char * owner,
              const char * group) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0, -1, EINVAL);
    PARAMETER_ASSERT((owner && strlen(owner) > 0) || (group && strlen(group) > 0), -1, EINVAL);

    try {
        fs->getFilesystem().setOwner(path, owner, group);
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsChmod(hdfsFS fs, const char * path, short mode) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0, -1, EINVAL);

    try {
        fs->getFilesystem().setPermission(path, mode);
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsUtime(hdfsFS fs, const char * path, tTime mtime, tTime atime) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0, -1, EINVAL);

    try {
        fs->getFilesystem().setTimes(path, mtime, atime);
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsTruncate(hdfsFS fs, const char * path, tOffset pos, int * shouldWait) {
    PARAMETER_ASSERT(fs && path && strlen(path) > 0 && pos >= 0 && shouldWait, -1, EINVAL);

    try {
        *shouldWait = !fs->getFilesystem().truncate(path, pos);
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

char * hdfsGetDelegationToken(hdfsFS fs, const char * renewer) {
    PARAMETER_ASSERT(fs && renewer && strlen(renewer) > 0, NULL, EINVAL);

    try {
        std::string token = fs->getFilesystem().getDelegationToken(renewer);
        return Strdup(token.c_str());
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return NULL;
}

void hdfsFreeDelegationToken(char * token) {
    if (!token) {
        return;
    }

    delete token;
}

int64_t hdfsRenewDelegationToken(hdfsFS fs, const char * token) {
    PARAMETER_ASSERT(fs && token && strlen(token) > 0, -1, EINVAL);

    try {
        return fs->getFilesystem().renewDelegationToken(token);
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

int hdfsCancelDelegationToken(hdfsFS fs, const char * token) {
    PARAMETER_ASSERT(fs && token && strlen(token) > 0, -1, EINVAL);

    try {
        fs->getFilesystem().cancelDelegationToken(token);
        return 0;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return -1;
}

static Namenode * hdfsGetConfiguredNamenodesInternal(const char * nameservice,
        int * size, shared_ptr<Config> conf) {
    std::vector<NamenodeInfo> namenodeInfos = NamenodeInfo::GetHANamenodeInfo(
                nameservice, *conf);

    if (namenodeInfos.empty()) {
        return NULL;
    }

    Namenode * retval = new Namenode[namenodeInfos.size()];

    for (size_t i = 0; i < namenodeInfos.size(); ++i) {
        if (namenodeInfos[i].getHttpAddr().empty()) {
            retval[i].http_addr = NULL;
        } else {
            retval[i].http_addr = Strdup(namenodeInfos[i].getHttpAddr().c_str());
        }

        if (namenodeInfos[i].getRpcAddr().empty()) {
            retval[i].rpc_addr = NULL;
        } else {
            retval[i].rpc_addr = Strdup(namenodeInfos[i].getRpcAddr().c_str());
        }
    }

    *size = namenodeInfos.size();
    return retval;
}

Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) {
    PARAMETER_ASSERT(nameservice && size, NULL, EINVAL);

    try {
        return hdfsGetConfiguredNamenodesInternal(nameservice, size,
                DefaultConfig().getConfig());
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return NULL;
}

Namenode * hdfsGetHANamenodesWithConfig(const char * conf,
                                        const char * nameservice, int * size) {
    PARAMETER_ASSERT(conf && strlen(conf) > 0 && nameservice && size, NULL, EINVAL);

    try {
        return hdfsGetConfiguredNamenodesInternal(nameservice, size,
                DefaultConfig(conf).getConfig());
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        handleException(Hdfs::current_exception());
    }

    return NULL;
}

void hdfsFreeNamenodeInformation(Namenode * namenodes, int size) {
    if (namenodes && size > 0) {
        for (int i = 0; i < size; ++i) {
            delete[] namenodes[i].http_addr;
            delete[] namenodes[i].rpc_addr;
        }
    }

    delete[] namenodes;
}

static void ConstructFileBlockLocation(Hdfs::BlockLocation & bl, BlockLocation * target) {
    memset(target, 0, sizeof(BlockLocation));
    target->corrupt = bl.isCorrupt();
    target->numOfNodes = bl.getNames().size();
    target->length = bl.getLength();
    target->offset = bl.getOffset();
    target->hosts = new char *[target->numOfNodes];
    memset(target->hosts, 0, sizeof(char) * target->numOfNodes);
    target->names = new char *[target->numOfNodes];
    memset(target->names, 0, sizeof(char) * target->numOfNodes);
    target->topologyPaths = new char *[target->numOfNodes];
    memset(target->topologyPaths, 0, sizeof(char) * target->numOfNodes);
    const std::vector<std::string> & hosts = bl.getHosts();
    const std::vector<std::string> & names = bl.getNames();
    const std::vector<std::string> & topologyPaths = bl.getTopologyPaths();

    for (int i = 0; i < target->numOfNodes; ++i) {
        target->hosts[i] = Strdup(hosts[i].c_str());
        target->names[i] = Strdup(names[i].c_str());
        target->topologyPaths[i] = Strdup(topologyPaths[i].c_str());
    }
}

BlockLocation * hdfsGetFileBlockLocations(hdfsFS fs, const char * path,
        tOffset start, tOffset length, int * numOfBlock) {
    PARAMETER_ASSERT(fs && numOfBlock && path && strlen(path), NULL, EINVAL);
    PARAMETER_ASSERT(start >= 0 && length > 0, NULL, EINVAL);
    BlockLocation * retval = NULL;
    int size = 0;

    try {
        std::vector<Hdfs::BlockLocation> locations = fs->getFilesystem().getFileBlockLocations(path, start, length);
        size = locations.size();
        retval = new BlockLocation[size];

        for (int i = 0; i < size; ++i) {
            ConstructFileBlockLocation(locations[i], &retval[i]);
        }

        *numOfBlock = size;
        return retval;
    } catch (const std::bad_alloc & e) {
        SetErrorMessage("Out of memory");
        hdfsFreeFileBlockLocations(retval, size);
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Hdfs::current_exception());
        hdfsFreeFileBlockLocations(retval, size);
        handleException(Hdfs::current_exception());
    }

    return NULL;
}

void hdfsFreeFileBlockLocations(BlockLocation * locations, int numOfBlock) {
    if (!locations) {
        return;
    }

    for (int i = 0; i < numOfBlock; ++i) {
        for (int j = 0; j < locations[i].numOfNodes; ++j) {
            delete [] locations[i].hosts[j];
            delete [] locations[i].names[j];
            delete [] locations[i].topologyPaths[j];
        }

        delete [] locations[i].hosts;
        delete [] locations[i].names;
        delete [] locations[i].topologyPaths;
    }

    delete [] locations;
}

#ifdef __cplusplus
}
#endif
