import { useNavigation } from "@react-navigation/native";
import * as Cause from "effect/Cause";
import { useCallback, useMemo, useState } from "react";
import { Alert, Platform, ScrollView, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AndroidScreenHeader } from "../../components/AndroidScreenHeader";
import { AppText as Text } from "../../components/AppText";
import { NativeStackScreenOptions } from "../../native/StackHeader";
import { useProjects, useThreadShells } from "../../state/entities";
import { projectEnvironment } from "../../state/projects";
import { useAtomCommand } from "../../state/use-atom-command";
import { SettingsSection } from "./components/SettingsSection";
import { SettingsSwitchRow } from "./components/SettingsSwitchRow";

function projectKey(project: { readonly environmentId: string; readonly id: string }): string {
  return `${project.environmentId}:${project.id}`;
}

export function SettingsProjectOrchestrationRouteScreen() {
  const navigation = useNavigation();
  const insets = useSafeAreaInsets();
  const projects = useProjects();
  const threads = useThreadShells();
  const updateProject = useAtomCommand(projectEnvironment.update, { reportFailure: false });
  const [savingKeys, setSavingKeys] = useState<ReadonlySet<string>>(() => new Set());
  const sortedProjects = useMemo(
    () => [...projects].sort((left, right) => left.title.localeCompare(right.title)),
    [projects],
  );

  const updateTrust = useCallback(
    async (project: (typeof projects)[number], trusted: boolean) => {
      const key = projectKey(project);
      if (savingKeys.has(key)) return;
      setSavingKeys((current) => new Set(current).add(key));
      try {
        const result = await updateProject({
          environmentId: project.environmentId,
          input: { projectId: project.id, agentOrchestrationTrusted: trusted },
        });
        if (result._tag === "Failure") {
          const error = Cause.squash(result.cause);
          Alert.alert(
            "Could not update agent orchestration",
            error instanceof Error ? error.message : "An unexpected error occurred.",
          );
        }
      } finally {
        setSavingKeys((current) => {
          const next = new Set(current);
          next.delete(key);
          return next;
        });
      }
    },
    [savingKeys, updateProject],
  );

  const requestTrustChange = useCallback(
    (project: (typeof projects)[number], trusted: boolean) => {
      if (trusted) {
        void updateTrust(project, true);
        return;
      }
      const directChildren = threads.filter(
        (thread) =>
          thread.environmentId === project.environmentId &&
          thread.projectId === project.id &&
          thread.agentParentThreadId != null &&
          thread.archivedAt === null,
      );
      Alert.alert(
        "Turn off agent orchestration?",
        [
          `Trusted Root threads in “${project.title}” will no longer be able to delegate.`,
          ...(directChildren.length > 0
            ? [
                `${directChildren.length} visible direct child ${directChildren.length === 1 ? "thread" : "threads"} will be stopped and settled if active:`,
                ...directChildren.map((child) => `• ${child.title}`),
              ]
            : ["Any active direct child agents will be stopped and settled."]),
        ].join("\n\n"),
        [
          { text: "Cancel", style: "cancel" },
          {
            text: "Turn Off",
            style: "destructive",
            onPress: () => void updateTrust(project, false),
          },
        ],
      );
    },
    [threads, updateTrust],
  );

  return (
    <View collapsable={false} className="flex-1 bg-sheet">
      {Platform.OS === "android" ? (
        <>
          <NativeStackScreenOptions options={{ headerShown: false }} />
          <AndroidScreenHeader title="Agent Orchestration" onBack={() => navigation.goBack()} />
        </>
      ) : null}
      <ScrollView
        contentInsetAdjustmentBehavior="automatic"
        showsVerticalScrollIndicator={false}
        className="flex-1"
        contentContainerClassName="gap-3 px-5 pt-4"
        contentContainerStyle={{ paddingBottom: Math.max(insets.bottom, 18) + 18 }}
      >
        <Text className="px-1 text-sm leading-normal text-foreground-muted">
          Trust is off by default. Enabling a project takes effect on the next Codex Root turn.
        </Text>
        <SettingsSection title="Projects">
          {sortedProjects.length === 0 ? (
            <Text className="p-4 text-sm text-foreground-muted">No projects available.</Text>
          ) : (
            sortedProjects.map((project) => (
              <SettingsSwitchRow
                key={projectKey(project)}
                disabled={savingKeys.has(projectKey(project))}
                icon="sparkles"
                label={project.title}
                subtitle={project.workspaceRoot}
                value={project.agentOrchestrationTrusted === true}
                onValueChange={(trusted) => requestTrustChange(project, trusted)}
              />
            ))
          )}
        </SettingsSection>
      </ScrollView>
    </View>
  );
}
