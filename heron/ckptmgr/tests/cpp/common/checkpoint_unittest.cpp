/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "common/checkpoint.h"
#include <string>

#include "proto/messages.h"
#include "gtest/gtest.h"

namespace heron {
namespace ckptmgr {

class CheckpointTest : public ::testing::Test {
 public:
  CheckpointTest() {}
  ~CheckpointTest() {}

  void SetUp() {}
  void TearDown() {}

  heron::proto::ckptmgr::SaveStateCheckpoint* createSaveMessage() {
    auto protomsg = new heron::proto::ckptmgr::SaveStateCheckpoint;
    auto instance = protomsg->mutable_instance();
    instance->mutable_instance_id()->assign("instance-1");
    instance->mutable_stmgr_id()->assign("stmgr-1");

    auto info = instance->mutable_info();
    info->set_task_id(1);
    info->mutable_component_name()->assign("component_name-1");
    info->set_component_index(1);

    auto checkpoint = protomsg->mutable_checkpoint();
    checkpoint->mutable_checkpoint_id()->assign("checkpoint-1");
    checkpoint->mutable_state()->assign("abcdefghijklmnopqrstuvwxyz");
    return protomsg;
  }

  heron::proto::ckptmgr::RestoreStateCheckpoint* createRestoreMessage() {
    auto protomsg = new heron::proto::ckptmgr::RestoreStateCheckpoint;
    auto instance = protomsg->mutable_instance();
    instance->mutable_instance_id()->assign("instance-2");
    instance->mutable_stmgr_id()->assign("stmgr-2");

    auto info = instance->mutable_info();
    info->set_task_id(2);
    info->mutable_component_name()->assign("component_name-2");
    info->set_component_index(2);

    protomsg->mutable_checkpoint_id()->assign("checkpoint-2");
    return protomsg;
  }
};

TEST_F(CheckpointTest, constructor1) {
  auto save_message = createSaveMessage();
  Checkpoint sckpt("topology-1", save_message);

  EXPECT_EQ(sckpt.getTopology(), "topology-1");
  EXPECT_EQ(sckpt.getCkptId(), "checkpoint-1");
  EXPECT_EQ(sckpt.getComponent(), "component_name-1");
  EXPECT_EQ(sckpt.getInstance(), "instance-1");
  EXPECT_EQ(sckpt.checkpoint(), save_message);
}

TEST_F(CheckpointTest, constructor2) {
  auto restore_message = createRestoreMessage();
  Checkpoint rckpt("topology-2", restore_message);

  EXPECT_EQ(rckpt.getTopology(), "topology-2");
  EXPECT_EQ(rckpt.getCkptId(), "checkpoint-2");
  EXPECT_EQ(rckpt.getComponent(), "component_name-2");
  EXPECT_EQ(rckpt.getInstance(), "instance-2");
  EXPECT_EQ(rckpt.checkpoint(), nullptr);
  EXPECT_EQ(rckpt.nbytes(), 0);
}

}  // namespace ckptmgr
}  // namespace heron

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
