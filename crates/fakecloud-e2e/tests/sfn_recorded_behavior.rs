//! Step Functions behavior replayed against what AWS itself returned for the same
//! definitions: what creation refuses and why, and for what runs, its status,
//! output, error, cause and history event types.

mod helpers;

use aws_sdk_sfn::error::ProvideErrorMetadata;
use aws_sdk_sfn::types::ExecutionStatus;
use helpers::TestServer;
use serde_json::Value;

const ROLE: &str = "arn:aws:iam::123456789012:role/fc-probe";

/// `{name, definition, recorded}`, as recorded from AWS.
const RECORDED: &str = r###"[
 {
  "name": "delim_spaces",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Output": " {% 1 %} "
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "\" {% 1 %} \"",
   "error": null,
   "cause": null
  }
 },
 {
  "name": "delim_open_only",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Output": "{% 1"
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: JSONata string must start with {% and end with %} at /States/A/Output'"
  }
 },
 {
  "name": "delim_text_around",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Output": "a {% 1 %} b"
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "\"a {% 1 %} b\"",
   "error": null,
   "cause": null
  }
 },
 {
  "name": "delim_inner",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Output": {
      "x": "{% 1 %} tail"
     }
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: JSONata string must start with {% and end with %} at /States/A/Output/x'"
  }
 },
 {
  "name": "mix_jp_in_jsonata_parameters",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Parameters": {
      "a": 1
     }
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The QueryLanguage is set to 'JSONata', but field 'Parameters' is only supported for the 'JSONPath' QueryLanguage at /States/A'"
  }
 },
 {
  "name": "mix_jp_in_jsonata_inputpath",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "InputPath": "$"
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The QueryLanguage is set to 'JSONata', but field 'InputPath' is only supported for the 'JSONPath' QueryLanguage at /States/A'"
  }
 },
 {
  "name": "mix_jp_in_jsonata_resultpath",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "ResultPath": "$.x"
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The QueryLanguage is set to 'JSONata', but field 'ResultPath' is only supported for the 'JSONPath' QueryLanguage at /States/A'"
  }
 },
 {
  "name": "mix_jp_dotdollar",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Output": {
      "a.$": "$.b"
     }
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "{\"a.$\":\"$.b\"}",
   "error": null,
   "cause": null
  }
 },
 {
  "name": "mix_jsonata_in_jp_output",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Output": {
      "a": 1
     }
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The QueryLanguage is set to 'JSONPath', but field 'Output' is only supported for the 'JSONata' QueryLanguage at /States/A'"
  }
 },
 {
  "name": "mix_jsonata_in_jp_args",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {},
     "End": true
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The QueryLanguage is set to 'JSONPath', but field 'Arguments' is only supported for the 'JSONata' QueryLanguage at /States/A, SCHEMA_VALIDATION_FAILED: Parameters field is required for resource ARN: arn:aws:states:::aws-sdk:sfn:listStateMachines at /States/A/Resource'"
  }
 },
 {
  "name": "state_level_ql_jp_in_jsonata",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "QueryLanguage": "JSONPath",
     "Parameters": {
      "a": 1
     }
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: 'QueryLanguage' can not be 'JSONPath' if set to 'JSONata' for whole state machine at /States/A, SCHEMA_VALIDATION_FAILED: The QueryLanguage is set to 'JSONata', but field 'Parameters' is only supported for the 'JSONPath' QueryLanguage at /States/A'"
  }
 },
 {
  "name": "state_level_ql_jsonata_in_jp",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "QueryLanguage": "JSONata",
     "Output": "{% 1 %}"
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "1",
   "error": null,
   "cause": null
  }
 },
 {
  "name": "jsonpath_state_in_jsonata_top_downgrade",
  "definition": {
   "QueryLanguage": "JSONPath",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "QueryLanguage": "JSONata",
     "Output": "{% 1 %}",
     "Next": null
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: There should only be one of the following fields: [Next, End] at /States/A'"
  }
 },
 {
  "name": "retry_task_args",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {
      "maxResults": "{% $states.input.missing %}"
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Arguments/maxResults' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "EvaluationFailed",
    "EvaluationFailed",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_task_output",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {},
     "Output": "{% $states.input.missing %}",
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Output' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_task_assign",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {},
     "Assign": {
      "x": "{% $states.input.missing %}"
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Assign/x' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_parallel_output",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Parallel",
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Pass",
         "End": true
        }
       }
      }
     ],
     "Output": "{% $states.input.missing %}",
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Output' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "ParallelStateEntered",
    "ParallelStateStarted",
    "PassStateEntered",
    "PassStateExited",
    "ParallelStateSucceeded",
    "EvaluationFailed",
    "ParallelStateStarted",
    "PassStateEntered",
    "PassStateExited",
    "ParallelStateSucceeded",
    "EvaluationFailed",
    "ParallelStateStarted",
    "PassStateEntered",
    "PassStateExited",
    "ParallelStateSucceeded",
    "EvaluationFailed",
    "ParallelStateFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_parallel_args",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Parallel",
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Pass",
         "End": true
        }
       }
      }
     ],
     "Arguments": "{% $states.input.missing %}",
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Arguments' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "ParallelStateEntered",
    "EvaluationFailed",
    "EvaluationFailed",
    "EvaluationFailed",
    "ParallelStateFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_map_items",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "Items": "{% $states.input.missing %}",
     "ItemProcessor": {
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Items' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "MapStateEntered",
    "EvaluationFailed",
    "EvaluationFailed",
    "EvaluationFailed",
    "MapStateFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_map_output",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "Items": "{% [1] %}",
     "ItemProcessor": {
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "Output": "{% $states.input.missing %}",
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Output' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "MapStateEntered",
    "MapStateStarted",
    "MapIterationStarted",
    "PassStateEntered",
    "PassStateExited",
    "MapIterationSucceeded",
    "MapStateSucceeded",
    "EvaluationFailed",
    "MapStateStarted",
    "MapIterationStarted",
    "PassStateEntered",
    "PassStateExited",
    "MapIterationSucceeded",
    "MapStateSucceeded",
    "EvaluationFailed",
    "MapStateStarted",
    "MapIterationStarted",
    "PassStateEntered",
    "PassStateExited",
    "MapIterationSucceeded",
    "MapStateSucceeded",
    "EvaluationFailed",
    "MapStateFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_named_query_err",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {
      "maxResults": "{% $states.input.missing %}"
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.QueryEvaluationError"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Arguments/maxResults' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "EvaluationFailed",
    "EvaluationFailed",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "catch_taskfailed_query",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {
      "maxResults": "{% $states.input.missing %}"
     },
     "Catch": [
      {
       "ErrorEquals": [
        "States.TaskFailed"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true,
     "Output": "{% 'caught' %}"
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Arguments/maxResults' returned nothing (undefined)."
  }
 },
 {
  "name": "catch_all_query_pass",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "Output": "{% $states.input.missing %}",
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true,
     "Output": "{% 'caught' %}"
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: Field 'Catch' is not supported at /States/A'"
  }
 },
 {
  "name": "catch_all_query_task_out",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {},
     "Output": "{% $states.input.missing %}",
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true,
     "Output": "{% 'caught' %}"
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "\"caught\"",
   "error": null,
   "cause": null,
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "TaskStateExited",
    "PassStateEntered",
    "PassStateExited",
    "ExecutionSucceeded"
   ]
  }
 },
 {
  "name": "catch_named_query_map",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "Items": "{% $states.input.missing %}",
     "ItemProcessor": {
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "Catch": [
      {
       "ErrorEquals": [
        "States.QueryEvaluationError"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true,
     "Output": "{% 'caught' %}"
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "\"caught\"",
   "error": null,
   "cause": null,
   "types": [
    "ExecutionStarted",
    "MapStateEntered",
    "EvaluationFailed",
    "MapStateFailed",
    "MapStateExited",
    "PassStateEntered",
    "PassStateExited",
    "ExecutionSucceeded"
   ]
  }
 },
 {
  "name": "catch_assign_bad",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {
      "maxResults": "{% $states.input.missing %}"
     },
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Assign": {
        "y": "{% $states.input.missing %}"
       },
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true,
     "Output": "{% 'caught' %}"
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Catch[0]/Assign/y' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "EvaluationFailed",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "wait_no_duration",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Wait",
     "End": true
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: This object should have only one of the following fields: [SecondsPath, Seconds, TimestampPath, Timestamp] at /States/A'"
  }
 },
 {
  "name": "wait_two_durations",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Wait",
     "Seconds": 1,
     "Timestamp": "2026-01-01T00:00:00Z",
     "End": true
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: There should only be one of the following fields: [SecondsPath, Seconds, TimestampPath, Timestamp] at /States/A'"
  }
 },
 {
  "name": "var_reserved",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Assign": {
      "states": 1
     }
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'RESERVED_VARIABLE_NAME: Cannot assign to reserved variable '$states'. at /States/A'"
  }
 },
 {
  "name": "var_bad_name",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Assign": {
      "1x": 1
     }
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'INVALID_VARIABLE_NAME: Invalid variable name '1x': the variable name contains invalid characters. at /States/A/Assign/1x'"
  }
 },
 {
  "name": "assign_nonobject",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Assign": 1
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: Expected value of type [OBJECT] at /States/A/Assign'"
  }
 },
 {
  "name": "sync_no_template_parallel_var",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "P",
   "States": {
    "P": {
     "Type": "Parallel",
     "Assign": {
      "a": "{% $states.result %}"
     },
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Pass",
         "End": true,
         "Assign": {
          "a": 1
         }
        }
       }
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "[{}]",
   "error": null,
   "cause": null
  }
 }
]"###;

async fn run_to_end(
    client: &aws_sdk_sfn::Client,
    name: &str,
    definition: &str,
) -> Result<aws_sdk_sfn::operation::describe_execution::DescribeExecutionOutput, String> {
    run_with_input(client, name, definition, "{}").await
}

async fn run_with_input(
    client: &aws_sdk_sfn::Client,
    name: &str,
    definition: &str,
    input: &str,
) -> Result<aws_sdk_sfn::operation::describe_execution::DescribeExecutionOutput, String> {
    let machine = client
        .create_state_machine()
        .name(name)
        .role_arn(ROLE)
        .definition(definition)
        .send()
        .await
        .map_err(|e| e.message().unwrap_or_default().to_string())?;
    let execution = client
        .start_execution()
        .state_machine_arn(machine.state_machine_arn())
        .input(input)
        .send()
        .await
        .expect("start execution");
    for _ in 0..200 {
        let described = client
            .describe_execution()
            .execution_arn(execution.execution_arn())
            .send()
            .await
            .expect("describe execution");
        if described.status() != &ExecutionStatus::Running {
            return Ok(described);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{name} did not finish");
}

/// Run every case, returning how each differs from its recording.
async fn mismatches_with(client: &aws_sdk_sfn::Client, cases: &[Value]) -> Vec<String> {
    let mut mismatches = Vec::new();
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let recorded = &case["recorded"];
        let definition = case["definition"].to_string();
        let input = case["input"].as_str().unwrap_or("{}");
        let machine = name.replace('_', "-");
        let result = run_with_input(client, &machine, &definition, input).await;
        let described = match (result, recorded["create_error"].as_str()) {
            (Err(actual), Some(expected)) => {
                if actual != expected {
                    mismatches.push(format!(
                        "{name}: {actual}
  recorded: {expected}"
                    ));
                }
                continue;
            }
            (Err(actual), None) => {
                mismatches.push(format!("{name}: creation refused: {actual}"));
                continue;
            }
            (Ok(_), Some(expected)) => {
                mismatches.push(format!("{name}: created, recorded: {expected}"));
                continue;
            }
            (Ok(described), None) => described,
        };
        let checks = [
            ("status", Some(described.status().as_str())),
            ("output", described.output()),
            ("error", described.error()),
            ("cause", described.cause()),
        ];
        for (field, actual) in checks {
            // Where the recording quotes the JVM's own parser, its message is
            // compared up to there.
            if field == "cause" {
                if let Some(prefix) = recorded["cause_starts_with"].as_str() {
                    if !actual.is_some_and(|cause| cause.starts_with(prefix)) {
                        mismatches.push(format!(
                            "{name}: cause: {actual:?}\n  recorded: {prefix}..."
                        ));
                    }
                    continue;
                }
            }
            if actual != recorded[field].as_str() {
                mismatches.push(format!(
                    "{name}: {field}: {actual:?}
  recorded: {}",
                    recorded[field]
                ));
            }
        }
        if let Some(expected) = recorded["types"].as_array() {
            let history = client
                .get_execution_history()
                .execution_arn(described.execution_arn())
                .max_results(100)
                .send()
                .await
                .expect("history");
            let actual: Vec<&str> = history
                .events()
                .iter()
                .map(|e| e.r#type().as_str())
                .collect();
            let expected: Vec<&str> = expected.iter().filter_map(Value::as_str).collect();
            if actual != expected {
                mismatches.push(format!(
                    "{name}: {actual:?}
  recorded: {expected:?}"
                ));
            }
        }
    }
    mismatches
}

#[tokio::test]
async fn definitions_behave_as_recorded_from_aws() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let cases: Vec<Value> = serde_json::from_str(RECORDED).unwrap();
    assert!(cases.len() >= 30);
    let mismatches = mismatches_with(&client, &cases).await;
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

/// `{name, definition, input?, recorded}`, recorded from AWS for the second review
/// round: integer settings, I/O failures after branches succeed, intrinsic and
/// Fail-path failures, JSONata Wait seconds, nested query languages, catchers.
const REVIEW_RECORDED: &str = r###"[
 {
  "name": "tfc_2_0",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "ItemProcessor": {
      "ProcessorConfig": {
       "Mode": "DISTRIBUTED",
       "ExecutionType": "EXPRESS"
      },
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "ToleratedFailureCount": 2.0,
     "End": true
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: Expected value of type Integer at /States/A/ToleratedFailureCount'"
  },
  "input": "[1,2]"
 },
 {
  "name": "tfc_2_5",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "ItemProcessor": {
      "ProcessorConfig": {
       "Mode": "DISTRIBUTED",
       "ExecutionType": "EXPRESS"
      },
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "ToleratedFailureCount": 2.5,
     "End": true
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: Expected value of type Integer at /States/A/ToleratedFailureCount'"
  },
  "input": "[1,2]"
 },
 {
  "name": "tfc_path_2_0",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "ItemProcessor": {
      "ProcessorConfig": {
       "Mode": "DISTRIBUTED",
       "ExecutionType": "EXPRESS"
      },
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "ItemsPath": "$.i",
     "ToleratedFailureCountPath": "$.t",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The ToleratedFailureCountPath field refers to value \"2.0\" which is not a valid integer: $.t"
  },
  "input": "{\"i\": [1], \"t\": 2.0}"
 },
 {
  "name": "tfc_path_2_5",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "ItemProcessor": {
      "ProcessorConfig": {
       "Mode": "DISTRIBUTED",
       "ExecutionType": "EXPRESS"
      },
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "ItemsPath": "$.i",
     "ToleratedFailureCountPath": "$.t",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The ToleratedFailureCountPath field refers to value \"2.5\" which is not a valid integer: $.t",
   "types": [
    "ExecutionStarted",
    "MapStateEntered",
    "MapStateStarted",
    "ExecutionFailed"
   ]
  },
  "input": "{\"i\": [1], \"t\": 2.5}"
 },
 {
  "name": "tfc_jsonata_2_0",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "ItemProcessor": {
      "ProcessorConfig": {
       "Mode": "DISTRIBUTED",
       "ExecutionType": "EXPRESS"
      },
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "Items": "{% [1] %}",
     "ToleratedFailureCount": "{% 2.0 %}",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "[1]",
   "error": null,
   "cause": null
  }
 },
 {
  "name": "tfc_jsonata_2_5",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "ItemProcessor": {
      "ProcessorConfig": {
       "Mode": "DISTRIBUTED",
       "ExecutionType": "EXPRESS"
      },
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "Items": "{% [1] %}",
     "ToleratedFailureCount": "{% 2.5 %}",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The ToleratedFailureCount field cannot be parsed as an integer: 2.5",
   "types": [
    "ExecutionStarted",
    "MapStateEntered",
    "MapStateStarted",
    "EvaluationFailed",
    "MapStateFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "parallel_resultselector_fail",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Parallel",
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Pass",
         "End": true
        }
       }
      }
     ],
     "ResultSelector": {
      "x.$": "$.missing"
     },
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONPath '$.missing' specified for the field 'x.$' could not be found in the input '[{}]'",
   "types": [
    "ExecutionStarted",
    "ParallelStateEntered",
    "ParallelStateStarted",
    "PassStateEntered",
    "PassStateExited",
    "ParallelStateSucceeded",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "map_resultselector_fail",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "ItemProcessor": {
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "ResultSelector": {
      "x.$": "$.missing"
     },
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONPath '$.missing' specified for the field 'x.$' could not be found in the input '[1]'",
   "types": [
    "ExecutionStarted",
    "MapStateEntered",
    "MapStateStarted",
    "MapIterationStarted",
    "PassStateEntered",
    "PassStateExited",
    "MapIterationSucceeded",
    "MapStateSucceeded",
    "ExecutionFailed"
   ]
  },
  "input": "[1]"
 },
 {
  "name": "parallel_outputpath_fail",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Parallel",
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Pass",
         "End": true
        }
       }
      }
     ],
     "OutputPath": "$.missing",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). Invalid path '$.missing' : Property ['missing'] not found in path $",
   "types": [
    "ExecutionStarted",
    "ParallelStateEntered",
    "ParallelStateStarted",
    "PassStateEntered",
    "PassStateExited",
    "ParallelStateSucceeded",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "parallel_ok",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Parallel",
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Pass",
         "End": true
        }
       }
      }
     ],
     "ResultSelector": {
      "x.$": "$[0]"
     },
     "End": true
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "{\"x\":{}}",
   "error": null,
   "cause": null,
   "types": [
    "ExecutionStarted",
    "ParallelStateEntered",
    "ParallelStateStarted",
    "PassStateEntered",
    "PassStateExited",
    "ParallelStateSucceeded",
    "ParallelStateExited",
    "ExecutionSucceeded"
   ]
  }
 },
 {
  "name": "task_param_intrinsic_retry",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Parameters": {
      "maxResults.$": "States.StringToJson($.x)"
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "ExecutionFailed"
   ],
   "cause_starts_with": "An error occurred while executing the state 'A' (entered at the event id #2). There was an error while evaluating the intrinsic function: States.StringToJson($.x). Cannot convert String to Json in States.StringToJson, caused by: "
  },
  "input": "{\"x\":\"not json\"}"
 },
 {
  "name": "task_param_path_retry",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Parameters": {
      "maxResults.$": "$.missing"
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONPath '$.missing' specified for the field 'maxResults.$' could not be found in the input '{}'",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "task_param_intrinsic_catch",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Parameters": {
      "maxResults.$": "States.StringToJson($.x)"
     },
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "ExecutionFailed"
   ],
   "cause_starts_with": "An error occurred while executing the state 'A' (entered at the event id #2). There was an error while evaluating the intrinsic function: States.StringToJson($.x). Cannot convert String to Json in States.StringToJson, caused by: "
  },
  "input": "{\"x\":\"not json\"}"
 },
 {
  "name": "intr_madd",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Parameters": {
      "maxResults.$": "States.MathAdd($.s, 1)"
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). There was an error while evaluating the intrinsic function: States.MathAdd($.s, 1). Invalid arguments in States.MathAdd",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "ExecutionFailed"
   ]
  },
  "input": "{\"s\":\"x\"}"
 },
 {
  "name": "intr_fmt",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Parameters": {
      "maxResults.$": "States.Format('{} {}', $.a)"
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). There was an error while evaluating the intrinsic function: States.Format('{} {}', $.a). Invalid arguments in States.Format: number of arguments do not match the occurrences of {}.",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "ExecutionFailed"
   ]
  },
  "input": "{\"a\":\"1\"}"
 },
 {
  "name": "intr_arange",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Parameters": {
      "maxResults.$": "States.ArrayRange(1, 2, 0)"
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). There was an error while evaluating the intrinsic function: States.ArrayRange(1, 2, 0). Invalid arguments in States.ArrayRange",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "jwait_1_5",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Wait",
     "Seconds": "{% 1.5 %}",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The Seconds field cannot be parsed as an integer: 1.5",
   "types": [
    "ExecutionStarted",
    "WaitStateEntered",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "jwait_2_0",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Wait",
     "Seconds": "{% 2.0 %}",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "{}",
   "error": null,
   "cause": null,
   "types": [
    "ExecutionStarted",
    "WaitStateEntered",
    "WaitStateExited",
    "ExecutionSucceeded"
   ]
  }
 },
 {
  "name": "jwait_neg",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Wait",
     "Seconds": "{% -1 %}",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). Seconds cannot be negative but was: -1",
   "types": [
    "ExecutionStarted",
    "WaitStateEntered",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "jwait_str",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Wait",
     "Seconds": "{% 'a' %}",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression ''a'' specified for the field 'Seconds' returned an unexpected result type. Expected 'number', but was 'string' for value: \"a\"",
   "types": [
    "ExecutionStarted",
    "WaitStateEntered",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "jwait_huge",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Wait",
     "Seconds": "{% 99999999999 %}",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). Wait state delay is too long.",
   "types": [
    "ExecutionStarted",
    "WaitStateEntered",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "jwait_big_ok",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Wait",
     "Seconds": "{% 0 %}",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "{}",
   "error": null,
   "cause": null,
   "types": [
    "ExecutionStarted",
    "WaitStateEntered",
    "WaitStateExited",
    "ExecutionSucceeded"
   ]
  }
 },
 {
  "name": "jwait_literal_1_5",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Wait",
     "Seconds": 1.5,
     "End": true
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: Expected value of type Integer at /States/A/Seconds'"
  }
 },
 {
  "name": "jpwait_secpath_1_5",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Wait",
     "SecondsPath": "$.s",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The SecondsPath parameter cannot be parsed as a long value: $.s == 1.5",
   "types": [
    "ExecutionStarted",
    "WaitStateEntered",
    "ExecutionFailed"
   ]
  },
  "input": "{\"s\":1.5}"
 },
 {
  "name": "fail_errorpath_missing",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Fail",
     "ErrorPath": "$.missing",
     "Cause": "c"
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONPath '$.missing' specified for the field 'ErrorPath' could not be found in the input '{}'",
   "types": [
    "ExecutionStarted",
    "FailStateEntered",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "fail_causepath_missing",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Fail",
     "Error": "e",
     "CausePath": "$.missing"
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONPath '$.missing' specified for the field 'CausePath' could not be found in the input '{}'",
   "types": [
    "ExecutionStarted",
    "FailStateEntered",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "fail_errorpath_number",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Fail",
     "ErrorPath": "$.n",
     "Cause": "c"
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The value referenced by ErrorPath must be a string, but the value was: 1",
   "types": [
    "ExecutionStarted",
    "FailStateEntered",
    "ExecutionFailed"
   ]
  },
  "input": "{\"n\":1}"
 },
 {
  "name": "fail_errorpath_invalid",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Fail",
     "ErrorPath": "notapath",
     "Cause": "c"
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The value must be a valid Reference Path or a valid intrinsic function call at /States/A/ErrorPath'"
  }
 },
 {
  "name": "fail_causepath_invalid",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Fail",
     "Error": "e",
     "CausePath": "notapath"
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The value must be a valid Reference Path or a valid intrinsic function call at /States/A/CausePath'"
  }
 },
 {
  "name": "jp_machine_parallel_jsonata_nested_jp",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Parallel",
     "QueryLanguage": "JSONata",
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Pass",
         "End": true,
         "QueryLanguage": "JSONPath"
        }
       }
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "[{}]",
   "error": null,
   "cause": null
  }
 },
 {
  "name": "jp_machine_map_jsonata_nested_jp",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "QueryLanguage": "JSONata",
     "Items": "{% [1] %}",
     "ItemProcessor": {
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true,
        "QueryLanguage": "JSONPath"
       }
      }
     },
     "End": true
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "[1]",
   "error": null,
   "cause": null
  }
 },
 {
  "name": "jsonata_machine_parallel_nested_jp",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Parallel",
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Pass",
         "End": true,
         "QueryLanguage": "JSONPath"
        }
       }
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: 'QueryLanguage' can not be 'JSONPath' if set to 'JSONata' for whole state machine at /States/A/Branches[0]/States/X'"
  }
 },
 {
  "name": "jsonata_machine_map_nested_jp",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "Items": "{% [1] %}",
     "ItemProcessor": {
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true,
        "QueryLanguage": "JSONPath"
       }
      }
     },
     "End": true
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: 'QueryLanguage' can not be 'JSONPath' if set to 'JSONata' for whole state machine at /States/A/ItemProcessor/States/X'"
  }
 },
 {
  "name": "jsonata_machine_top_jp",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "QueryLanguage": "JSONPath"
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: 'QueryLanguage' can not be 'JSONPath' if set to 'JSONata' for whole state machine at /States/A'"
  }
 },
 {
  "name": "catch_task_query_jsonata",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {
      "maxResults": "{% $states.input.missing %}"
     },
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "{\"Error\":\"States.QueryEvaluationError\",\"Cause\":\"An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Arguments/maxResults' returned nothing (undefined).\"}",
   "error": null,
   "cause": null,
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "EvaluationFailed",
    "TaskStateExited",
    "PassStateEntered",
    "PassStateExited",
    "ExecutionSucceeded"
   ]
  }
 },
 {
  "name": "catch_parallel_branch_fail",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Parallel",
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Fail",
         "Error": "E",
         "Cause": "C"
        }
       }
      }
     ],
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "{\"Error\":\"E\",\"Cause\":\"C\"}",
   "error": null,
   "cause": null,
   "types": [
    "ExecutionStarted",
    "ParallelStateEntered",
    "ParallelStateStarted",
    "FailStateEntered",
    "ParallelStateFailed",
    "ParallelStateExited",
    "PassStateEntered",
    "PassStateExited",
    "ExecutionSucceeded"
   ]
  }
 },
 {
  "name": "catch_map_resultselector",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "ItemProcessor": {
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "ResultSelector": {
      "x.$": "$.missing"
     },
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.Runtime",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONPath '$.missing' specified for the field 'x.$' could not be found in the input '[1]'",
   "types": [
    "ExecutionStarted",
    "MapStateEntered",
    "MapStateStarted",
    "MapIterationStarted",
    "PassStateEntered",
    "PassStateExited",
    "MapIterationSucceeded",
    "MapStateSucceeded",
    "ExecutionFailed"
   ]
  },
  "input": "[1]"
 },
 {
  "name": "catch_jsonata_parallel_output",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Parallel",
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Pass",
         "End": true
        }
       }
      }
     ],
     "Output": "{% $states.input.missing %}",
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "{\"Error\":\"States.QueryEvaluationError\",\"Cause\":\"An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Output' returned nothing (undefined).\"}",
   "error": null,
   "cause": null,
   "types": [
    "ExecutionStarted",
    "ParallelStateEntered",
    "ParallelStateStarted",
    "PassStateEntered",
    "PassStateExited",
    "ParallelStateSucceeded",
    "EvaluationFailed",
    "ParallelStateFailed",
    "ParallelStateExited",
    "PassStateEntered",
    "PassStateExited",
    "ExecutionSucceeded"
   ]
  }
 }
]"###;

#[tokio::test]
async fn review_definitions_behave_as_recorded_from_aws() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let cases: Vec<Value> = serde_json::from_str(REVIEW_RECORDED).unwrap();
    assert!(cases.len() >= 30);
    let mismatches = mismatches_with(&client, &cases).await;
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

/// Recorded: a Parallel or Map retried as a whole sees its attempt as
/// `$$.State.RetryCount`, and the time it was first entered as `EnteredTime`.
#[tokio::test]
async fn retried_parallel_and_map_see_their_retry_count() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let check = serde_json::json!({"StartAt":"C","States":{
        "C":{"Type":"Choice","Choices":[{"Variable":"$.rc","NumericEquals":2,"Next":"OK"}],"Default":"F"},
        "OK":{"Type":"Pass","End":true},
        "F":{"Type":"Fail","Error":"E"}
    }});
    let retry =
        serde_json::json!([{"ErrorEquals":["States.ALL"],"MaxAttempts":2,"IntervalSeconds":1}]);
    let selector = serde_json::json!({"rc.$":"$$.State.RetryCount","et.$":"$$.State.EnteredTime"});
    let parallel = serde_json::json!({"StartAt":"A","States":{"A":{
        "Type":"Parallel","Parameters":selector,"Branches":[check],"Retry":retry,"End":true
    }}});
    let map = serde_json::json!({"StartAt":"A","States":{"A":{
        "Type":"Map","ItemSelector":selector,"ItemProcessor":check,"Retry":retry,"End":true
    }}});
    let jsonata = serde_json::json!({"QueryLanguage":"JSONata","StartAt":"A","States":{"A":{
        "Type":"Parallel",
        "Arguments":{"rc":"{% $states.context.State.RetryCount %}","et":"{% $states.context.State.EnteredTime %}"},
        "Branches":[{"StartAt":"C","States":{
            "C":{"Type":"Choice","Choices":[{"Condition":"{% $states.input.rc = 2 %}","Next":"OK"}],"Default":"F"},
            "OK":{"Type":"Pass","End":true},
            "F":{"Type":"Fail","Error":"E"}
        }}],
        "Retry":retry,
        "End":true
    }}});
    for (name, definition, input, starts) in [
        ("retry-parallel", parallel, "{}", "ParallelStateStarted"),
        ("retry-map", map, "[1]", "MapStateStarted"),
        (
            "retry-jsonata-parallel",
            jsonata,
            "{}",
            "ParallelStateStarted",
        ),
    ] {
        let described = run_with_input(&client, name, &definition.to_string(), input)
            .await
            .unwrap();
        assert_eq!(described.status(), &ExecutionStatus::Succeeded, "{name}");
        let output: Value = serde_json::from_str(described.output().unwrap()).unwrap();
        assert_eq!(output[0]["rc"], 2, "{name}: {output}");
        let history = client
            .get_execution_history()
            .execution_arn(described.execution_arn())
            .max_results(100)
            .send()
            .await
            .unwrap();
        let entered: Vec<&str> = history
            .events()
            .iter()
            .filter_map(|e| e.state_entered_event_details())
            .filter(|d| d.name() == "C")
            .filter_map(|d| d.input())
            .collect();
        assert_eq!(entered.len(), 3, "{name}: {entered:?}");
        // Every attempt saw the same EnteredTime.
        let times: Vec<Value> = entered
            .iter()
            .map(|input| serde_json::from_str::<Value>(input).unwrap()["et"].clone())
            .collect();
        assert!(times.iter().all(|t| t == &times[0]), "{name}: {times:?}");
        let started = history
            .events()
            .iter()
            .filter(|e| e.r#type().as_str() == starts)
            .count();
        assert_eq!(started, 3, "{name}");
    }
}

#[tokio::test]
async fn evaluation_failures_name_the_nested_field_as_location() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let definition = serde_json::json!({
        "QueryLanguage": "JSONata",
        "StartAt": "A",
        "States": {"A": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
            "Arguments": {"maxResults": "{% $states.input.missing %}"},
            "End": true
        }}
    })
    .to_string();
    let described = run_to_end(&client, "location", &definition).await.unwrap();
    let history = client
        .get_execution_history()
        .execution_arn(described.execution_arn())
        .send()
        .await
        .unwrap();
    let raw: Vec<_> = history
        .events()
        .iter()
        .filter(|e| e.r#type().as_str() == "EvaluationFailed")
        .collect();
    let details = raw[0].evaluation_failed_event_details().expect("details");
    assert_eq!(details.location(), Some("Arguments/maxResults"));
}

#[tokio::test]
async fn s3_get_object_fails_as_recorded_on_binary_and_oversized_bodies() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let s3 = server.s3_client().await;
    s3.create_bucket().bucket("recorded").send().await.unwrap();
    s3.put_object()
        .bucket("recorded")
        .key("bin")
        .body((0u8..=255).collect::<Vec<u8>>().into())
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket("recorded")
        .key("big")
        .body(vec![b'x'; 300_000].into())
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket("recorded")
        .key("fits")
        .body(vec![b'x'; 200_000].into())
        .send()
        .await
        .unwrap();
    let definition = |key: &str| {
        serde_json::json!({"StartAt": "T", "States": {"T": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
            "Parameters": {"Bucket": "recorded", "Key": key},
            "End": true
        }}})
        .to_string()
    };

    let binary = run_to_end(&client, "binary", &definition("bin"))
        .await
        .unwrap();
    assert_eq!(binary.error(), Some("S3.InvalidContent"));
    assert_eq!(binary.cause(), Some("Failed to convert 'Body' to string"));

    let big = run_to_end(&client, "big", &definition("big"))
        .await
        .unwrap();
    assert_eq!(big.error(), Some("States.DataLimitExceeded"));
    assert_eq!(
        big.cause(),
        Some(
            "The state/task 'aws-sdk:s3' returned a result with a size exceeding the maximum \
             number of bytes service limit."
        )
    );

    let fits = run_to_end(&client, "fits", &definition("fits"))
        .await
        .unwrap();
    assert_eq!(fits.status(), &ExecutionStatus::Succeeded);
}
