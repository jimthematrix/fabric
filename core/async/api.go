package async

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"golang.org/x/net/context"

	core "github.com/hyperledger/fabric/core"
	"github.com/hyperledger/fabric/core/chaincode"
	"github.com/hyperledger/fabric/core/rest"
	pb "github.com/hyperledger/fabric/protos"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

type Connector interface {
	SystemName() string
	RuntimeFlags() [][]string
	Start() error
	Close() error
}

type ConnectorsList struct {
	sync.RWMutex
	connectors []Connector
}

func (cl *ConnectorsList) AddConnectorImpl(c Connector) {
	cl.Lock()

	cl.connectors = append(cl.connectors, c)

	cl.Unlock()
}

func (cl *ConnectorsList) GetConnectorImpls() []Connector {
	return cl.connectors
}

var ExternalConnectors ConnectorsList

// serverOpenchain is a variable that holds the pointer to the
// underlying ServerOpenchain object. serverDevops is a variable that holds
// the pointer to the underlying Devops object. This is necessary due to
// how the gocraft/web package implements context initialization.
var serverOpenchain *rest.ServerOpenchain
var serverDevops *core.Devops

type AsyncRequest struct {
	Jsonrpc *string           `json:"jsonrpc,omitempty"`
	Method  *string           `json:"method,omitempty"`
	Params  *pb.ChaincodeSpec `json:"params,omitempty"`
	ID      *rpcID            `json:"id,omitempty"`
}

type rpcID struct {
	StringValue *string
	IntValue    *int64
}

func (id *rpcID) UnmarshalJSON(b []byte) error {
	var err error
	s, n := "", int64(0)

	if err = json.Unmarshal(b, &s); err == nil {
		id.StringValue = &s
		return nil
	}
	if err = json.Unmarshal(b, &n); err == nil {
		id.IntValue = &n
		return nil
	}
	return fmt.Errorf("cannot unmarshal %s into Go value of type int64 or string", string(b))
}

func (id *rpcID) MarshalJSON() ([]byte, error) {
	if id.StringValue != nil {
		return json.Marshal(id.StringValue)
	}
	if id.IntValue != nil {
		return json.Marshal(id.IntValue)
	}
	return nil, errors.New("cannot marshal rpcID")
}

var logger = logging.MustGetLogger("Async Messaging API")

func ProcessChaincode(msgBody []byte) {
	logger.Info("Async Messaging API processing chaincode request...")

	// Payload must conform to the following structure
	var requestPayload AsyncRequest

	var err error

	// Decode the request payload as an rpcRequest structure.	There will be an
	// error here if the incoming JSON is invalid (e.g. missing brace or comma).
	err = json.Unmarshal(msgBody, &requestPayload)
	if err != nil {
		// Format the error appropriately
		logger.Errorf("Error unmarshalling chaincode request payload: %s", err)

		return
	}

	// Insure that the JSON method string is present and is either deploy, invoke or query
	if requestPayload.Method == nil {
		// If the request is not a notification, produce a response.
		logger.Error("Missing JSON RPC 2.0 method string.")

		return
	} else if (*(requestPayload.Method) != "deploy") && (*(requestPayload.Method) != "invoke") && (*(requestPayload.Method) != "query") {
		logger.Error("Requested method does not exist.")

		return
	}

	//
	// Confirm the requested chaincode method and execute accordingly
	//

	if *(requestPayload.Method) == "deploy" {

		//
		// Chaincode deployment was requested
		//

		// Payload params field must contain a ChaincodeSpec message
		if requestPayload.Params == nil {
			logger.Error("Client must supply ChaincodeSpec for chaincode deploy request.")

			return
		}

		// Extract the ChaincodeSpec from the params field
		deploySpec := requestPayload.Params

		// Process the chaincode deployment request and record the result
		err = processChaincodeDeploy(deploySpec)
	} else {

		//
		// Chaincode invocation/query was reqested
		//

		// Because chaincode invocation/query requests require a ChaincodeInvocationSpec
		// message instead of a ChaincodeSpec message, we must initialize it here
		// before  proceeding.
		invokequeryPayload := &pb.ChaincodeInvocationSpec{ChaincodeSpec: requestPayload.Params}

		// Payload params field must contain a ChaincodeSpec message
		if invokequeryPayload.ChaincodeSpec == nil {
			logger.Error("Client must supply ChaincodeSpec for chaincode invoke or query request.")

			return
		}

		// Process the chaincode invoke/query request and record the result
		err = processChaincodeInvokeOrQuery(*(requestPayload.Method), invokequeryPayload)
	}

	// Make a clarification in the invoke response message, that the transaction has been successfully submitted but not completed
	if *(requestPayload.Method) == "invoke" {
		logger.Infof("Successfully submitted invoke transaction")
	} else {
		logger.Infof("Successfully %s chaincode", *(requestPayload.Method))
	}

	return
}

// processChaincodeDeploy triggers chaincode deploy and returns a result or an error
func processChaincodeDeploy(spec *pb.ChaincodeSpec) error {
	logger.Info("Deploying chaincode...")

	// Check that the ChaincodeID is not nil.
	if spec.ChaincodeID == nil {
		logger.Error("Payload must contain a ChaincodeID.")

		return errors.New("Payload does not contain a ChaincodeID")
	}

	// If the peer is running in development mode, confirm that the Chaincode name
	// is not left blank. If the peer is running in production mode, confirm that
	// the Chaincode path is not left blank. This is necessary as in development
	// mode, the chaincode is identified by name not by path during the deploy
	// process.
	if viper.GetString("chaincode.mode") == chaincode.DevModeUserRunsChaincode {
		//
		// Development mode -- check chaincode name
		//

		// Check that the Chaincode name is not blank.
		if spec.ChaincodeID.Name == "" {
			// Format the error appropriately for further processing
			logger.Error("Chaincode name may not be blank in development mode.")

			return errors.New("Chaincode name may not be blank in development mode.")
		}
	} else {
		//
		// Network mode -- check chaincode path
		//

		// Check that the Chaincode path is not left blank.
		if spec.ChaincodeID.Path == "" {
			logger.Error("Chaincode path may not be blank.")

			return errors.New("Chaincode path may not be blank.")
		}
	}

	// Check that the CtorMsg is not left blank.
	if (spec.CtorMsg == nil) || (spec.CtorMsg.Function == "") {
		logger.Error("Payload must contain a CtorMsg with a Chaincode function name.")

		return errors.New("Payload must contain a CtorMsg with a Chaincode function name.")
	}

	//
	// Check if security is enabled
	//

	if core.SecurityEnabled() {
		// User registrationID must be present inside request payload with security enabled
		chaincodeUsr := spec.SecureContext
		if chaincodeUsr == "" {
			logger.Error("Must supply username for chaincode when security is enabled.")

			return errors.New("Must supply username for chaincode when security is enabled.")
		}

		// Retrieve the REST data storage path
		// Returns /var/hyperledger/production/client/
		localStore := getRESTFilePath()

		// Check if the user is logged in before sending transaction
		if _, err := os.Stat(localStore + "loginToken_" + chaincodeUsr); err == nil {
			// No error returned, therefore token exists so user is already logged in
			logger.Infof("Local user '%s' is already logged in. Retrieving login token.", chaincodeUsr)

			// Read in the login token
			token, err := ioutil.ReadFile(localStore + "loginToken_" + chaincodeUsr)
			if err != nil {
				logger.Errorf("Fatal error when reading client login token: %s", err)

				return errors.New("Fatal error when reading client login token")
			}

			// Add the login token to the chaincodeSpec
			spec.SecureContext = string(token)

			// If privacy is enabled, mark chaincode as confidential
			if viper.GetBool("security.privacy") {
				spec.ConfidentialityLevel = pb.ConfidentialityLevel_CONFIDENTIAL
			}
		} else {
			// Check if the token is not there and fail
			if os.IsNotExist(err) {
				logger.Error("User not logged in. Use the '/registrar' endpoint to obtain a security token.")

				return errors.New("User not logged in. Use the '/registrar' endpoint to obtain a security token.")
			}
			// Unexpected error
			logger.Errorf("Unexpected fatal error when checking for client login token: %s", err)

			return errors.New("Unexpected fatal error when checking for client login token")
		}
	}

	//
	// Trigger the chaincode deployment through the devops service
	//
	chaincodeDeploymentSpec, err := serverDevops.Deploy(context.Background(), spec)

	//
	// Deployment failed
	//

	if err != nil {
		logger.Errorf("Error when deploying chaincode: %s", err)

		return errors.New(fmt.Sprintf("Error when deploying chaincode: %s", err))
	}

	//
	// Deployment succeeded
	//

	// Clients will need the chaincode name in order to invoke or query it, record it
	chainID := chaincodeDeploymentSpec.ChaincodeSpec.ChaincodeID.Name

	//
	// Output correctly formatted response
	//

	logger.Infof("Successfully deployed chainCode: %s", chainID)

	return nil
}

// processChaincodeInvokeOrQuery triggers chaincode invoke or query and returns a result or an error
func processChaincodeInvokeOrQuery(method string, spec *pb.ChaincodeInvocationSpec) error {
	logger.Infof("Async %s chaincode...", method)

	// Check that the ChaincodeID is not nil.
	if spec.ChaincodeSpec.ChaincodeID == nil {
		// Format the error appropriately for further processing
		logger.Error("Payload must contain a ChaincodeID.")

		return errors.New("Payload must contain a ChaincodeID.")
	}

	// Check that the Chaincode name is not blank.
	if spec.ChaincodeSpec.ChaincodeID.Name == "" {
		logger.Error("Chaincode name may not be blank.")

		return errors.New("Chaincode name may not be blank.")
	}

	// Check that the CtorMsg is not left blank.
	if (spec.ChaincodeSpec.CtorMsg == nil) || (spec.ChaincodeSpec.CtorMsg.Function == "") {
		logger.Error("Payload must contain a CtorMsg with a Chaincode function name.")

		return errors.New("Payload must contain a CtorMsg with a Chaincode function name.")
	}

	//
	// Check if security is enabled
	//

	if core.SecurityEnabled() {
		// User registrationID must be present inside request payload with security enabled
		chaincodeUsr := spec.ChaincodeSpec.SecureContext
		if chaincodeUsr == "" {
			logger.Error("Must supply username for chaincode when security is enabled.")

			return errors.New("Must supply username for chaincode when security is enabled.")
		}

		// Retrieve the REST data storage path
		// Returns /var/hyperledger/production/client/
		localStore := getRESTFilePath()

		// Check if the user is logged in before sending transaction
		if _, err := os.Stat(localStore + "loginToken_" + chaincodeUsr); err == nil {
			// No error returned, therefore token exists so user is already logged in
			logger.Infof("Local user '%s' is already logged in. Retrieving login token.\n", chaincodeUsr)

			// Read in the login token
			token, err := ioutil.ReadFile(localStore + "loginToken_" + chaincodeUsr)
			if err != nil {
				logger.Errorf("Fatal error when reading client login token: %s", err)

				return errors.New(fmt.Sprintf("Fatal error when reading client login token: %s", err))
			}

			// Add the login token to the chaincodeSpec
			spec.ChaincodeSpec.SecureContext = string(token)

			// If privacy is enabled, mark chaincode as confidential
			if viper.GetBool("security.privacy") {
				spec.ChaincodeSpec.ConfidentialityLevel = pb.ConfidentialityLevel_CONFIDENTIAL
			}
		} else {
			// Check if the token is not there and fail
			if os.IsNotExist(err) {
				logger.Error("User not logged in. Use the '/registrar' endpoint to obtain a security token.")

				return errors.New("User not logged in. Use the '/registrar' endpoint to obtain a security token.")
			}
			// Unexpected error
			logger.Errorf("Unexpected fatal error when checking for client login token: %s", err)

			return errors.New(fmt.Sprintf("Unexpected fatal error when checking for client login token: %s", err))
		}
	}

	// Check the method that is being requested and execute either an invoke or a query
	if method == "invoke" {

		//
		// Trigger the chaincode invoke through the devops service
		//

		resp, err := serverDevops.Invoke(context.Background(), spec)

		//
		// Invocation failed
		//

		if err != nil {
			// Replace " characters with ' within the chaincode response
			errVal := strings.Replace(err.Error(), "\"", "'", -1)

			logger.Errorf("Error when invoking chaincode: %s", errVal)

			return errors.New(fmt.Sprintf("Error when invoking chaincode: %s", errVal))
		}

		//
		// Invocation succeeded
		//

		// Clients will need the txuuid in order to track it after invocation, record it
		txuuid := string(resp.Msg)

		// Make a clarification in the invoke response message, that the transaction has been successfully submitted but not completed
		logger.Infof("Successfully submitted invoke transaction with txuuid (%s)", txuuid)
	}

	if method == "query" {

		//
		// Trigger the chaincode query through the devops service
		//

		resp, err := serverDevops.Query(context.Background(), spec)

		//
		// Query failed
		//

		if err != nil {
			// Replace " characters with ' within the chaincode response
			errVal := strings.Replace(err.Error(), "\"", "'", -1)

			logger.Errorf("Error when querying chaincode: %s", errVal)

			return errors.New(fmt.Sprintf("Error when querying chaincode: %s", errVal))
		}

		//
		// Query succeeded
		//

		// Clients will need the returned value, record it
		val := string(resp.Msg)

		logger.Infof("Successfully queried chaincode: %s", val)
	}

	return nil
}

// getRESTFilePath is a helper function to retrieve the local storage directory
// of client login tokens.
func getRESTFilePath() string {
	localStore := viper.GetString("peer.fileSystemPath")
	if !strings.HasSuffix(localStore, "/") {
		localStore = localStore + "/"
	}
	localStore = localStore + "client/"
	return localStore
}

func StartOpenchainAsyncAgent(server *rest.ServerOpenchain, devops *core.Devops) {
	// Record the pointer to the underlying ServerOpenchain and Devops objects.
	serverOpenchain = server
	serverDevops = devops

	var connector Connector

	txsSystem := viper.GetString("transactions-queue")

	if txsSystem != "" {
		for _, c := range ExternalConnectors.connectors {
			if txsSystem == c.SystemName() {
				connector = c
				err1 := connector.Start()

				if err1 != nil {
					connectorLogger.Error(fmt.Sprintf("Failed to initialize transactions queue connector: %v. %v", txsSystem, err1))
				} else {
					defer func() {
						if err2 := connector.Close(); err2 != nil {
							connectorLogger.Error(fmt.Sprintf("Failed to close transactions queue connector: %v. %v", txsSystem, err2))
						} else {
							connectorLogger.Info("Successfully close connection with %v", txsSystem)
						}
					}()
				}

				break
			}
		}
	}
}
