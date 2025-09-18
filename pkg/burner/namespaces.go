// Copyright 2020 The Kube-burner Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package burner

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/kube-burner/kube-burner/pkg/config"
	"github.com/kube-burner/kube-burner/pkg/util"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
)

// Cleanup resources specific to kube-burner with in a given list of namespaces
func CleanupNamespacesUsingGVR(ctx context.Context, ex JobExecutor, namespacesToDelete []string) {
	for _, namespace := range namespacesToDelete {
		labelSelector := fmt.Sprintf("kube-burner-job=%s", ex.Name)
		for _, obj := range ex.objects {
			if config.IsChurnEnabled(ex.Job) && obj.Churn {
				CleanupNamespaceResourcesUsingGVR(ctx, ex, obj, namespace, labelSelector)
			}
		}
		waitForDeleteNamespacedResources(ctx, ex, namespace, ex.objects, labelSelector)
	}
}

func CleanupNamespaceResourcesUsingGVR(ctx context.Context, ex JobExecutor, obj *object, namespace string, labelSelector string) {
	resourceInterface := ex.dynamicClient.Resource(obj.gvr).Namespace(namespace)
	resources, err := resourceInterface.List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	log.Infof("Deleting %ss labeled with %s in %s", obj.Kind, labelSelector, namespace)
	if err != nil {
		log.Errorf("Unable to list %vs in %v: %v", obj.Kind, namespace, err)
		return
	}
	for _, item := range resources.Items {
		if err := resourceInterface.Delete(ctx, item.GetName(), metav1.DeleteOptions{PropagationPolicy: ptr.To(metav1.DeletePropagationBackground)}); err != nil {
			if !errors.IsNotFound(err) {
				log.Errorf("Error deleting %v/%v in %v: %v", item.GetKind(), item.GetName(), namespace, err)
			}
		}
	}
}

// countObjectsInNamespaces counts all churn-enabled objects across the given namespaces
func countObjectsInNamespaces(ctx context.Context, ex JobExecutor, namespaces []string) int {
	totalObjects := 0
	labelSelector := fmt.Sprintf("kube-burner-job=%s", ex.Name)

	for _, namespace := range namespaces {
		for _, obj := range ex.objects {
			if config.IsChurnEnabled(ex.Job) && obj.Churn && obj.namespaced {
				resourceInterface := ex.dynamicClient.Resource(obj.gvr).Namespace(namespace)
				resources, err := resourceInterface.List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
				if err != nil {
					log.Errorf("Unable to list %vs in %v: %v", obj.Kind, namespace, err)
					continue
				}
				totalObjects += len(resources.Items)
			}
		}
	}
	return totalObjects
}

// deletePercentageOfObjects deletes a specific percentage of objects across all namespaces
func deletePercentageOfObjects(ctx context.Context, ex JobExecutor, namespaces []string, percentage int) []objectToDelete {
	labelSelector := fmt.Sprintf("kube-burner-job=%s", ex.Name)
	var allObjects []objectToDelete

	// Collect all objects across all namespaces
	for _, namespace := range namespaces {
		for _, obj := range ex.objects {
			if config.IsChurnEnabled(ex.Job) && obj.Churn && obj.namespaced {
				resourceInterface := ex.dynamicClient.Resource(obj.gvr).Namespace(namespace)
				resources, err := resourceInterface.List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
				if err != nil {
					log.Errorf("Unable to list %vs in %v: %v", obj.Kind, namespace, err)
					continue
				}
				for _, item := range resources.Items {
					// Extract iteration and replica from labels
					labels := item.GetLabels()
					iteration := 0
					replica := 1

					if iterStr, exists := labels[config.KubeBurnerLabelJobIteration]; exists {
						if i, err := strconv.Atoi(iterStr); err == nil {
							iteration = i
						}
					}

					if repStr, exists := labels[config.KubeBurnerLabelReplica]; exists {
						if r, err := strconv.Atoi(repStr); err == nil {
							replica = r
						}
					}

					allObjects = append(allObjects, objectToDelete{
						namespace: namespace,
						name:      item.GetName(),
						obj:       obj,
						iteration: iteration,
						replica:   replica,
					})
				}
			}
		}
	}

	if len(allObjects) == 0 {
		log.Info("No churn-enabled objects found to delete")
		return []objectToDelete{}
	}

	// Calculate number of objects to delete
	numToDelete := int(math.Max(float64(percentage*len(allObjects)/100), 1))

	// Determine object type for logging (assuming all objects are the same type for churn)
	var objectType string
	if len(allObjects) > 0 {
		objectType = allObjects[0].obj.Kind
	} else {
		objectType = "objects"
	}

	log.Infof("Deleting %d out of %d %s objects (%d%%)", numToDelete, len(allObjects), objectType, percentage)

	// Randomly shuffle and select objects to delete
	rand.Shuffle(len(allObjects), func(i, j int) {
		allObjects[i], allObjects[j] = allObjects[j], allObjects[i]
	})

	var deletedObjects []objectToDelete

	// Delete the selected objects
	for i := 0; i < numToDelete && i < len(allObjects); i++ {
		objToDelete := allObjects[i]
		resourceInterface := ex.dynamicClient.Resource(objToDelete.obj.gvr).Namespace(objToDelete.namespace)
		if err := resourceInterface.Delete(ctx, objToDelete.name, metav1.DeleteOptions{PropagationPolicy: ptr.To(metav1.DeletePropagationBackground)}); err != nil {
			if !errors.IsNotFound(err) {
				log.Errorf("Error deleting %v/%v in %v: %v", objToDelete.obj.Kind, objToDelete.name, objToDelete.namespace, err)
			}
		} else {
			log.Debugf("Deleted %s/%s in namespace %s", objToDelete.obj.Kind, objToDelete.name, objToDelete.namespace)
			deletedObjects = append(deletedObjects, objToDelete)
		}
	}

	if len(deletedObjects) > 0 {
		log.Infof("Successfully deleted %d %s objects", len(deletedObjects), objectType)
	}

	return deletedObjects
}

type objectToDelete struct {
	namespace string
	name      string
	obj       *object
	iteration int
	replica   int
}

// recreateDeletedObjects recreates only the specific objects that were deleted
func recreateDeletedObjects(ctx context.Context, ex JobExecutor, deletedObjects []objectToDelete) {
	// Determine object type for logging
	var objectType string
	if len(deletedObjects) > 0 {
		objectType = deletedObjects[0].obj.Kind
	} else {
		objectType = "objects"
	}

	log.Infof("Re-creating %d deleted %s objects", len(deletedObjects), objectType)

	recreatedCount := 0

	for _, objToDelete := range deletedObjects {
		// Find the object template from the executor
		var objTemplate *object
		for _, obj := range ex.objects {
			if obj.gvr == objToDelete.obj.gvr && obj.Churn {
				objTemplate = obj
				break
			}
		}

		if objTemplate == nil {
			log.Errorf("Could not find object template for %s", objToDelete.obj.Kind)
			continue
		}

		// Create the object with the same iteration and replica as the deleted one
		ex.recreateSingleObject(ctx, objTemplate, objToDelete.namespace, objToDelete.iteration, objToDelete.replica)
		recreatedCount++
		log.Debugf("Recreated %s/%s in namespace %s", objToDelete.obj.Kind, objToDelete.name, objToDelete.namespace)
	}

	if recreatedCount > 0 {
		log.Infof("Successfully recreated %d %s objects", recreatedCount, objectType)
	}
}

// waitForDeletedObjects waits for the deleted objects to be fully removed
func waitForDeletedObjects(ctx context.Context, ex JobExecutor, deletedObjects []objectToDelete) {
	err := wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		allDeleted := true
		for _, objToDelete := range deletedObjects {
			resourceInterface := ex.dynamicClient.Resource(objToDelete.obj.gvr).Namespace(objToDelete.namespace)
			_, err := resourceInterface.Get(ctx, objToDelete.name, metav1.GetOptions{})
			if err == nil {
				// Object still exists
				allDeleted = false
				log.Debugf("Waiting for %s/%s in %s to be deleted", objToDelete.obj.Kind, objToDelete.name, objToDelete.namespace)
			} else if !errors.IsNotFound(err) {
				// Some other error occurred
				return false, err
			}
			// If error is NotFound, the object is deleted which is what we want
		}
		return allDeleted, nil
	})
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Fatalf("Timeout waiting for objects to be deleted: %v", err)
		}
		log.Errorf("Error waiting for objects to be deleted: %v", err)
	}
}

// Cleanup non-namespaced resources using executor list
func CleanupNonNamespacedResourcesUsingGVR(ctx context.Context, ex JobExecutor, object *object, labelSelector string) {
	log.Infof("Deleting non-namespace %v with selector %v", object.Kind, labelSelector)
	resourceInterface := ex.dynamicClient.Resource(object.gvr)
	resources, err := resourceInterface.List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		log.Debugf("Unable to list resources for object: %v error: %v. Hence skipping it", object.Object, err)
		return
	}
	util.DeleteNonNamespacedResources(ctx, resources, resourceInterface)
}

func waitForDeleteNamespacedResources(ctx context.Context, ex JobExecutor, namespace string, objects []*object, labelSelector string) {
	err := wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		allDeleted := true
		for _, obj := range objects {
			// If churning is enabled and object doesn't have churning enabled we skip that object from deletion wait
			if config.IsChurnEnabled(ex.Job) && !obj.Churn {
				continue
			}
			if obj.namespaced {
				resourceInterface := ex.dynamicClient.Resource(obj.gvr).Namespace(namespace)
				objList, err := resourceInterface.List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
				if err != nil {
					return false, err
				}
				if len(objList.Items) > 0 {
					allDeleted = false
					log.Debugf("Waiting for %d objects labeled with %s in %s to be deleted",
						len(objList.Items), labelSelector, namespace)
				}
			}
		}
		return allDeleted, nil
	})
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Fatalf("Timeout waiting for objects to be deleted: %v", err)
		}
		log.Errorf("Error waiting for objects to be deleted: %v", err)
	}
}
