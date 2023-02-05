package waitingpod

import (
	"errors"
	"math"
	"strings"
	"sync"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type NodeScoreList []NodeScore

type NodeScore struct {
	Name  string
	Score int64
}

type NodeToStatusMap map[string]*Status

type NodePluginScores struct {
	Name       string
	Scores     []PluginScore
	TotalScore int64
}

type PluginScore struct {
	Name  string
	Score int64
}

type Code int

const (
	Success Code = iota
	Error
	Unschedulable
	UnschedulableAndUnresolvable
	Wait
	Skip
)

var codes = []string{"Success", "Error", "Unschedulable", "UnschedulableAndUnresolvable", "Wait", "Skip"}

func (c Code) String() string {
	return codes[c]
}

const (
	MaxNodeScore  int64 = 100
	MinNodeScore  int64 = 0
	MaxTotalScore int64 = math.MaxInt64
)

type PodsToActivate struct {
	sync.Mutex
	Map map[string]*v1.Pod
}

func (s *PodsToActivate) Clone() framework.StateData {
	return s
}

func NewPodsToActivate() *PodsToActivate {
	return &PodsToActivate{Map: make(map[string]*v1.Pod)}
}

type Status struct {
	code         Code
	reasons      []string
	err          error
	failedPlugin string
}

func (s *Status) WithError(err error) *Status {
	s.err = err
	return s
}

func (s *Status) Code() Code {
	if s == nil {
		return Success
	}
	return s.code
}

func (s *Status) Message() string {
	if s == nil {
		return ""
	}
	return strings.Join(s.Reasons(), ",")
}

func (s *Status) SetFailedPlugin(plugin string) {
	s.failedPlugin = plugin
}

func (s *Status) WithFailedPlugin(plugin string) *Status {
	s.SetFailedPlugin(plugin)
	return s
}

func (s *Status) FailedPlugin() string {
	return s.failedPlugin
}

func (s *Status) Reasons() []string {
	if s.err != nil {
		return append([]string{s.err.Error()}, s.reasons...)
	}
	return s.reasons
}

func (s *Status) AppendReason(reason string) {
	s.reasons = append(s.reasons, reason)
}

func (s *Status) IsSuccess() bool {
	return s.Code() == Success
}

func (s *Status) IsWait() bool {
	return s.Code() == Wait
}

func (s *Status) IsSkip() bool {
	return s.Code() == Skip
}

func (s *Status) IsUnschedulable() bool {
	code := s.Code()
	return code == Unschedulable || code == UnschedulableAndUnresolvable
}

func (s *Status) AsError() error {
	if s.IsSuccess() || s.IsWait() || s.IsSkip() {
		return nil
	}
	if s.err != nil {
		return s.err
	}
	return errors.New(s.Message())
}

func (s *Status) Equal(x *Status) bool {
	if s == nil || x == nil {
		return s.IsSuccess() && x.IsSuccess()
	}
	if s.code != x.code {
		return false
	}
	if !cmp.Equal(s.err, x.err, cmpopts.EquateErrors()) {
		return false
	}
	if !cmp.Equal(s.reasons, x.reasons) {
		return false
	}
	return cmp.Equal(s.failedPlugin, x.failedPlugin)
}

func NewStatus(code Code, reasons ...string) *Status {
	s := &Status{
		code:    code,
		reasons: reasons,
	}
	return s
}

func AsStatus(err error) *Status {
	if err == nil {
		return nil
	}
	return &Status{
		code: Error,
		err:  err,
	}
}
